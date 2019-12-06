from flask import Flask
from flask import jsonify
from flask import request
import json
import sys
# flask's request isn't for sending request to other sites
import requests
import math
import random
from flask_apscheduler import APScheduler
import copy



app = Flask(__name__)

# Store key
d = {}

# Node's address
ADDRESS = ""

# the vector clock index in context, add 1 if used as keyshard ID
# view.index(ADDRESS) % (len(view) / repl_factor)
keyshard_ID = 0

# the column of this node in the vector clock
# math.ceiling((view.index(ADDRESS) + 1) / (len(view) / repl_factor)) - 1
node_ID = 0

# causal context
# when giving context back to the client, only modify one keyshard
# leave the other keyshards untouched
context = []

# replica factor
repl_factor = 1

# add to this event log for write/del
# the event is a list
# format goes [context[keyshard_ID], 'PUT/DEL', 'key', 'value (leave blank if del)']
event_log = []

# to keep live easy, increase for every event added to event_log
event_counter = 0

# acks = a record of which node got which gossip, so we don't have to send a huge list of event_log every time
# key is the index of the other node
acks = {}

# Current view
view = []

#current shard map
shard_map = []

# if we're gonna do something that might screw up if gossip is running around like view change
# (since gossips are on a different thread)
# then set this to False, then periodic gossip will stop, then set this to True again to make it run
shouldDoGossip = True

#counter for number of view-change operations that have completed. Once we have gotten blobs from every other node, the view change is considered completed
view_change_counter = 0

# creates a 2D array of 0's with size [keyshards][repl_factor]
# keyshards = number of nodes / repl_factor = number of keyshards
# the vector clock for this keyshard is context[keyshard_ID]
# the lamport clock of this node is context[keyshard_ID][node_ID]
def initialize_context():
    return [[0 for _ in range(repl_factor)] for _ in range(int(len(view) / repl_factor))]


# is context1 strictly larger than context2
# [0, 0, 0, 0] and [0, 0, 0, 0] returns FALSE
def areContextStrictlyLarger(context1, context2):
    larger = False
    for index in range(len(context2)):
        if context1[index] < context2[index]:
            return False
        if context1[index] > context2[index]:
            larger = True
    return larger

# is context larger or equal
# [0, 0, 0, 0] return true, use this to determine if we can accept client's request
def areContextLarger(context1, context2):
    for index in range(len(context2)):
        if context1[index] < context2[index]:
            return False
    return True


def updateVectorClock():
    context[keyshard_ID][node_ID] = context[keyshard_ID][node_ID] + 1

# compare 2 context
def areContextConcurrent(context1, context2):
    has_smaller, has_larger = False, False
    for index in range(len(context2)):
        if context1[index] < context2[index]:
            has_smaller = True
        elif context1[index] > context2[index]:
            has_larger = True
    return has_smaller and has_larger

# if it's not larger and not concurrent, it's smaller


##EXPERIMENTAL FEATURE:
# using xor-distance rather than modulo to distribute keys
# this is a drop-in replacement. Simply replace every use of view[hash(key) % len(view)] with xordist_get_addr(key)
# advantages: resharading does not require as many keys to change location during a reshard
# advantages: lookup is O(n) in the number of nodes rather than constant-time... but for n < 10,000 this is still practically nothing
def xordist_get_addr(key):
    key_hash = hash(key)
    dist_min = hash(ADDRESS) ^ key_hash
    addr_min = ADDRESS
    for node in iter(
            view):  # find the minimum of distances(measured with XOR) between the hash of the address and the hash of the key
        if hash(node) ^ key_hash < dist_min:
            dist_min = hash(node) ^ key_hash
            addr_min = node
    return addr_min


@app.route("/")
def default():
    return "CSE 138 Lab 2."

# Insert and update key
@app.route('/kv-store/keys/<keyname>', methods=['PUT'])
def putKey(keyname):
    bin = hash(keyname) % len(shard_map)
    # Check if keyname over 50 characters
    if len(keyname) > 50:
        return jsonify(error='Key is too long ', message='Error in PUT'), 201

    # Get request
    req = request.get_json()
    global event_counter
    if bin == keyshard_ID:
        updateVectorClock()
        event_counter = event_counter + 1
        event_log.append([copy.deepcopy(context[keyshard_ID]), 'PUT', keyname, event_counter, req.get('value')])
        if not req or "value" not in req:
            return jsonify(error='value is missing', message='Error in PUT'), 400

        # Check if key already exists
        if keyname in d.keys():
            d[keyname]['value'] = req.get('value')
            d[keyname]['context'] = copy.deepcopy(context[keyshard_ID])
            return jsonify(message='Updated successfully', replaced=True), 200
        # Add new key
        else:
            d[keyname] = {}
            d[keyname]['value'] = req.get('value')
            d[keyname]['context'] = copy.deepcopy(context[keyshard_ID])
            d[keyname]['exist'] = True
            return jsonify(message='Added successfully', replaced=False), 200

    else:
        return forward_request_multiple(request, shard_map[bin])




# Get key
@app.route('/kv-store/keys/<keyname>', methods=['GET'])
def getKey(keyname):
    
    bin = hash(keyname) % len(view)
    global event_counter
    req = request.get_json()
    tempContext = req['causal-context']
    
    # Check if key exists.
    if keyname in d and d[keyname]['exists'] is True:
         # Check client context and initialize if needed.
        if tempContext == '' or len(tempContext) != len(context) or len(tempContext[0]) != len(context[0]):
            tempContext = initialize_context()
 
        # Violates causal causality as client context is greater.
        if areContextLarger(tempContext[keyshard_ID], d[keyname]['context']):
            tempContext[keyshard_ID] = context[keyshard_ID]
            return jsonify(error = 'Unable to satisfy request.', message = 'Error in <HTTP Method.>'), 503
        
        # Return if client context is equal or smaller.
        elif areContextLarger(tempContext[keyshard_ID], context[keyshard_ID]) or not areContextConcurrent(tempContext[keyshard_ID], context[keyshard_ID]):
            payload = {"doesExist": True, "message": 'Retrieved successfully', "value": d[keyname]['value']} 
            # Use our own context.
            tempContext[keyshard_ID] = context[keyshard_ID]
            d[keyname]['context'] = tempContext[keyshard_ID]
            payload['causal-context'] = tempContext
            # If it's not directly from client, add the correct address
            if 'from_node' in request.headers:
                payload['address'] = ADDRESS
            # Update our vector clock.
            updateVectorClock()
            # Update event counter.
            event_counter = event_counter + 1
            event_log.append([copy.deepcopy(context[keyshard_ID]), 'GET', keyname, event_counter, 'poop'])
             # If it's not directly from client, add the correct address
            if 'from_node' in request.headers:
                payload['address'] = ADDRESS
            return jsonify(payload), 200
        else:
            pass
            
    else:
        if 'from_node' in request.headers:
            # Node does not exist.
            return jsonify(doesExist=False, error='Key does not exist', message='Error in GET'), 404
        # Forward it to the right node.
        else:
            final_response = None
            final_status_code = None
            node_is_alive = False
            # forward this to at least one node in destiny keyshard
            for index in range(bin, len(view), int(len(view) / repl_factor)):
                # try forwarding it
                response, status_code = forward_request(request, view[index])
                # if it succeeds
                if status_code == 200:
                    # just return it
                    return response, status_code
                # if it's key not found error, maybe they haven't gossiped yet
                elif status_code == 404:
                    # but record it anyways
                    final_response = response
                    final_status_code = status_code
                    # and say that at least someone is alive
                    node_is_alive = True
            # if someone is alive, even if it doesn't succeed, return their response
            if node_is_alive:
                return final_response, final_status_code
            # if all of the nodes failed, nak
            else:
                return jsonify({'message': 'Error in PUT', 'error': 'Unable to satisfy request'}), 503


# Get shard (replicas not yet implemented)
@app.route('/kv-store/shards/<id>', methods=['GET'])
def getShard(id):
    bin = int(id)
    if bin < 0 or bin >= len(shard_map):
        return jsonify({"message": "Node does not exist"})
    if bin == keyshard_ID:
        return jsonify({"message": 'Shard information retrieved successfully', "shard-id": bin, "key-count": len(d), "causal-context": jsonify({"c": context}), "replicas": shard_map[bin]})
    else:
        return forward_request_multiple(request, shard_map[bin])


# Get all shards
@app.route('/kv-store/shards', methods=['GET'])
def getAllShards():
    return jsonify({"message": 'Shard information retrieved successfully', "causal-context": context, "shard-ids": range(0,len(shard_map))}),200
     
# Delete key
#TODO: concurrnet request, immediate gossip
@app.route('/kv-store/keys/<keyname>', methods=['DELETE'])
def deleteKey(keyname):
    # calculate the shard this index belongs to
    bin = hash(keyname) % len(shard_map)
    global event_counter
    # if it belongs to this keyshard
    if keyshard_ID == bin:
        # Get request
        req = request.get_json()
        tempContext = req['causal-context']
        # client doesn't have a context or client's context is outdated for the current view
        if tempContext == '' or len(tempContext) != len(context) or type(tempContext[0]) is not list or len(tempContext[0]) != len(context[0]) or type(tempContext[0][0]) is not int:
            # treat it as a 0 context
            tempContext = initialize_context()
        # if this key exist
        if areContextStrictlyLarger(tempContext[keyshard_ID], context[keyshard_ID]):
            tempContext[keyshard_ID] = context[keyshard_ID]
            # refuse to serve: we do not know what other things does the client know about and could
            # possibly violate causality, so we give them OUR most-updated context
            return json.dumps({'message': 'Error in PUT', 'error': 'Unable to satisfy request',
                               'causal-context': tempContext}), 503
        # if client's context is strictly larger than ours
        # if it's equal or smaller, that means we can serve them
        elif areContextLarger(tempContext[keyshard_ID], context[keyshard_ID]) or not areContextConcurrent(
                tempContext[keyshard_ID], context[keyshard_ID]):
            if keyname in d.keys() and d[keyname]['exist']:
                # update our own vector clock
                updateVectorClock()
                # make this key not exist
                d[keyname]['exist'] = False
                # since it's equal or smaller, just use our context
                tempContext[keyshard_ID] = context[keyshard_ID]
                # update the key's context in d
                d[keyname]['context'] = tempContext[keyshard_ID]
                # give back their stuff
                payload = {'doesExist': True, 'message': 'Deleted successfully'}
                if 'from_node' in request.headers:
                    payload['address'] = ADDRESS
                payload['causal-context'] = tempContext
                # append to event_log, 'poop' is just to make all entries equal length
                event_counter = event_counter + 1
                event_log.append([copy.deepcopy(context[keyshard_ID]), 'DEL', keyname, event_counter, 'poop'])
                return jsonify(payload), 200
            else:
                tempContext[keyshard_ID] = context[keyshard_ID]
                # this does not exist
                return jsonify(doesExist=False, error='Key does not exist', message='Error in DELETE', context=tempContext), 404
        # it's concurrent
        else:
            pass
    else:
        # for all node that is in the destination keyshard
        final_response = None
        final_status_code = None
        node_is_alive = False
        # forward this to at least one node in destiny keyshard
        for node in shard_map[bin]:
            # try forwarding it
            response, status_code = forward_request(request, node)
            # if it succeeds
            if status_code == 200:
                # just return it
                return response, status_code
            # if it's key not found error, maybe they haven't gossiped yet
            elif status_code == 404:
                # but record it anyways
                final_response = response
                final_status_code = status_code
                # and say that at least someone is alive
                node_is_alive = True
        # if someone is alive, even if it doesn't succeed, return their response
        if node_is_alive:
            return final_response, final_status_code
        # if all of the nodes failed, nak
        else:
            return jsonify({'message': 'Error in PUT', 'error': 'Unable to satisfy request'}), 503


# Get key count
@app.route('/kv-store/key-count', methods=['GET'])
def getKeyCount():
    return jsonify({"message": "Key count retrieved successfully", "key-count": len(d)}), 200


@app.route('/get-view', methods=['GET'])
def get_view():
    return jsonify(view), 200


@app.route('/key-distribute', methods=['PUT'])
def startDistribution():
    return key_distribute(), 200


def updateContext(newContext):
    global context
    # if the new context is larger than own context
    if areContextLarger(newContext, context[keyshard_ID]):
        # just use the new context
        context[keyshard_ID] = newContext
    # if they're concurrent, take the larger one of them all
    elif areContextConcurrent(newContext, context[keyshard_ID]):
        for index in range(len(newContext)):
            if newContext[index] > context[keyshard_ID][index]:
                context[keyshard_ID][index] = newContext[index]



# periodic gossip receiving end
@app.route('/gossip', methods=['PUT'])
def periodicGossipReceived():
    log = request.get_json()
    counter = -1
    global event_counter
    for entry in log:
        # if this key exists
        if entry[2] in d.keys():
            # use the context in d, the last-updated context
            tempContext = d[entry[2]]['context']
        else:
            # use 0,0,0,0 for last updated context for this key
            tempContext = [0 for _ in context[keyshard_ID]]
        # is the event context strictly > the last updated context for this key?
        if areContextStrictlyLarger(entry[0], tempContext):
            # if yes then update the key
            if entry[2] not in d.keys():
                d[entry[2]] = {}
            # if it's del then just mark exist false for deleting instead of actually deleting it
            if entry[1] == 'DEL':
                d[entry[2]]['exist'] = False
            elif entry[1] == 'PUT':
                # if it's put then put value in
                d[entry[2]]['value'] = entry[4]
                d[entry[2]]['exist'] = True
            d[entry[2]]['context'] = entry[0]
            # since this event changes d, put this into event log again in the case of partial partition
            event_counter = event_counter + 1
            event_log.append([entry[0], entry[1], entry[2],  event_counter, entry[3]])
        # if this entry is concurrent with the last update
        elif areContextConcurrent(entry[0], tempContext):
            replace = False
            # check for every index
            for index in range(len(entry[0])):
                # if the first different vector clock has the entry's event be larger
                if entry[0][index] > tempContext[index]:
                    # that means this is a change that the nodes in the front of the list knows about
                    # while the nodes in the back don't, and since we're listening to whoever
                    # is in the front of the list, we take it
                    replace = True
                    break
                # if the first different vector clock has the entry's event be smaller
                elif entry[0][index] < tempContext[index]:
                    # that means this is a change that we know yet the nodes further down the list don't know yet
                    # so ignore it
                    break
            # same thing as above
            if replace:
                if entry[2] not in d.keys():
                    d[entry[2]] = {}
                if entry[1] == 'DEL':
                    d[entry[2]]['exist'] = False
                else:
                    d[entry[2]]['value'] = entry[4]
                    d[entry[2]]['exist'] = True
                d[entry[2]]['context'] = entry[0]
                event_counter = event_counter + 1
                event_log.append([entry[0], entry[1], entry[2], event_counter, entry[3]])
        # update the context regardless of whether we take it, new context is just the two contexts combined
        # taking the larger number from each
        updateContext(entry[0])
        # set counter to the last event we process
        counter = entry[3]
    # send an ack to the sender node, saying "we've received everything up to counter"
    if counter > -1:
        requests.put(url="http://" + request.headers['from_node'] + "/ack/" + str(view.index(ADDRESS)),
                     headers={'from_node': ADDRESS, "Content-Type": "application/json"},
                     data=json.dumps({"counter": counter}))
    # so flask shuts up
    return ""

class Poop(dict):
    def __str__(self):
        return json.dumps(self, indent=4, sort_keys=True)

@app.route('/debug', methods=['GET'])
def debug():
        return "\nd\t" + str(Poop(d)) + "\nevent log\t" + str(event_log) + \
               "\ncontext\t" + str(context) + "\nacks\t" + \
               str(acks) + "\nkeyshard_ID\t" + str(keyshard_ID) + "\nnode_ID\t" + str(node_ID) + \
               "\nevent_counter\t" + str(event_counter) + "\nview\t" + str(view)


# acks of periodic gossip
# index is the index of the sender in view, because I'm lazy
@app.route('/ack/<index>', methods=['PUT'])
def ackReceived(index):
    # use index for ack
    acks[index] = request.get_json()['counter']
    # get the minimum counter from the list of acks
    minimum = acks[min(acks, key=acks.get)]
    # remove the entry that everyone has received and processed
    while len(event_log) > 0 and event_log[0][3] <= minimum:
        event_log.pop(0)
    # so flask shuts up
    return ""


# Helper method to rehash and redistribute keys according to the new view
# Returns either an error message detailing which node failed to accept their new key(s) or the string "ok"
# This method tries to do everything in order, rather than broadcasting
@app.route('/kv-store/view-change', methods=['PUT'])
# perform a view change
def viewChange():
    global view
    global shard_map
    global keyshard_ID
    shouldDoGossip = False #turn off gossip until we are done
    req = request.get_json()
    new_view = req['view']
    new_repl_factor = int(req['repl_factor'])
    new_shard_map = []
    for index in range(0,math.floor(len(view)/repl_factor)):
        new_shard_map.append(view[index*repl_factor:(index+1)*repl_factor])
    keyshard_ID = math.floor(view.index(ADDRESS) / repl_factor)
    view = new_view.split(',')
    shard_map = new_shard_map
    # if we need to, notify all the other nodes of this view change
    if 'from_node' not in request.headers:
        for node in view:
            if node != ADDRESS:
                forward_request(request, node)
        for node in view:
            if node != ADDRESS:
                requests.put(url="http://" + node + "/key-distribute",
                             headers={'from_node': ADDRESS})
        key_distribute()
        view_map = []
        for shard in shard_map:
            try:
                response = requests.get(url="http://" + shard[0] + "/kv-store/key-count")
                count = response.json()['key-count']
            except Exception:
                return "Node " + shard[0] + " did not respond to a request for its key count", 400
            view_map.append({"shard-id": shard_map.index(shard), "key-count": count, "replicas": shard})
        return jsonify(message="View change successful", shards=view_map), 200
    else:
        return "ok", 200

#inserts a bunch of keys at once. Done during a view change. It is assumed that this is done only once, and all the keys have their vector clocks set to 0
#expects the message body to be a JSON dict to be merged with the current dict, with the same structure
@app.route('/kv-store/insert-blob', methods = ['PUT'])
def insertBlob():
    global view_change_counter
    global d
    req = request.get_json()
    for key in list(req.keys()):
        #first check if the newly supplied value is newer than the one we have, and if so replace. Otherwise we leave it alone
        if key in d:
            if areContextStrictlyLarger(req[key]['context'],d[key]['context']):
                d[key] = req[key]
        else:
            d[key] = req[key]
    view_change_counter = view_change_counter + 1
    if view_change_counter == len(view): #received shit from everyone, now we're done and we can set all the vector clocks to 0 and do gossip again
        for key in d.keys():
            for i in range(0,len(d[key]['context'])): #reset the vector clock
                d[key]['context'][i] = 0
        shouldDoGossip = True
    return "ok",200


# helper method to rehash and redistribute keys according to the new view
# returns either an error message detailing which node failed to accept their new key(s) or the string "ok"
# this method tries to do everything in order, rather than broadcasting
def key_distribute():
    #sends blobs to everyone in the new view
    for index in range(0,len(shard_map)):
        to_send = {}
        for key in list(d.keys()):
            if hash(key) % len(shard_map) == index: 
                to_send[key] = d[key]
        for node in shard_map[index]: #send the blob to every member of the shard, so that we start with a clean slate everywhere
            try:
                requests.put(new_addr + "/kv-store/insert-blob", headers = {'from_node': ADDRESS},
                            data = jsonify(to_send))
            except Exception:
                return Exception
        #delete the keys locally once the blob is sent
        for key in to_send.keys():
            del d[key]

##EXPERIMENTAL FEATURE
# does the same thing as the above method, but adapted for xordist
def xordist_key_distribute():
    for key in iter(d):
        new_addr = xordist_get_addr(key)
        if new_addr != ADDRESS:  # if the key no longer belongs here, send it where it belongs
            try:
                requests.put(new_addr + "/kv-store/keys/" + key, headers={'from_node': ADDRESS},
                             data=jsonify({value: d[key]}))
                del d[key]  # delete the key
            except Exception:
                return Exception
    return "ok"


# forwards a request to the given address
def forward_request(request, node):
    # get the headers since request is immutable
    headers = {key: value for (key, value) in request.headers}
    # if it's not from another node but needs to be forwarded
    if 'from_node' not in request.headers:
        # mark that this is forwarded from this node
        headers['from_node'] = ADDRESS
    try:
        response = requests.request(
            method=request.method,
            url=request.url.replace(request.host, node),
            headers=headers,
            data=request.get_data(),
            timeout=20)
        return response.json(), response.status_code
    except ConnectionError:
        return jsonify(error='Node ' + node + " is down", message='Error in ' + request.method), 503
    except requests.exceptions.ConnectionError:
        return jsonify(error='Node ' + node + " is down", message='Error in ' + request.method), 503

#same as above, but tries each node in nodes until one of them responds
def forward_request_multiple(request,nodes):
    # get the headers since request is immutable
    headers = {key: value for (key, value) in request.headers}
    # if it's not from another node but needs to be forwarded
    if 'from_node' not in request.headers:
        # mark that this is forwarded from this node
        headers['from_node'] = ADDRESS
    for node in nodes:
        try:
            response = requests.request(
                method=request.method,
                url=request.url.replace(request.host, node),
                headers=headers,
                data=request.get_data(),
                timeout=20)
            return response.json(), response.status_code
        except ConnectionError:
            continue
        except requests.exceptions.ConnectionError:
            continue
    return "None of the nodes received the request",400


@app.before_first_request
def before_first_request():
    scheduler = APScheduler()
    scheduler.init_app(app)
    app.apscheduler.add_job(func=periodicGossip, trigger='interval', seconds=10, id='0')
    scheduler.start()
    # initialize the acks dict
    if len(acks.keys()) < repl_factor - 1:
        for node in shard_map[keyshard_ID]:
            if node != ADDRESS:
                acks[str(view.index(node))] = -1

def periodicGossip():
    # if there's no replica, just clear up event_log
    if repl_factor == 1:
        global event_log
        event_log = []
    else:
        # if there's event to be sent and we're not doing something else
        if len(event_log) > 0 and shouldDoGossip:
            # for every node that is in the same keyshard as this node
            for node in shard_map[keyshard_ID]:
                # if it's not this node
                if node != ADDRESS:
                    # put the gossip request
                    try:
                        requests.put(url="http://" + node + "/gossip",
                                     headers={'from_node': ADDRESS, "Content-Type": "application/json"},
                                     # the list comprehension just means only send the ones that the target
                                     # node has not seen
                                     data=json.dumps([entry for entry in event_log if entry[3] > acks[str(view.index(node))]]))
                    except ConnectionError:
                        pass
                    except requests.exceptions.ConnectionError:
                        pass


if __name__ == "__main__":
    app.debug = True
    ADDRESS = sys.argv[1]
    view = sys.argv[2].split(',')
    repl_factor = int(sys.argv[3])
    keyshard_ID = math.floor(view.index(ADDRESS) / repl_factor)
    for index in range(0,math.floor(len(view)/repl_factor)):
        shard_map.append(view[index*repl_factor:(index+1)*repl_factor])
    node_ID = shard_map[keyshard_ID].index(ADDRESS)
    context = initialize_context()
    app.run(host='0.0.0.0', port=13800)
