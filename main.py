from flask import Flask
from flask import jsonify
from flask import request
import json
import sys
# flask's request isn't for sending request to other sites
import requests
import math
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


# creates a 2D array of 0's with size [keyshards][repl_factor]
# keyshards = number of nodes / repl_factor = number of keyshards
# the vector clock for this keyshard is context[keyshard_ID]
# the lamport clock of this node is context[keyshard_ID][node_ID]
def initialize_context():
    return [[0 for _ in range(repl_factor)] for _ in range(int(len(view) / repl_factor))]


# is context1 > than the compared context
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
    bin = hash(keyname) % int(len(view) / repl_factor)
    # Check if keyname over 50 characters
    if len(keyname) > 50:
        return jsonify(error='Key is too long ', message='Error in PUT'), 201

    # Get request
    req = request.get_json()
    global event_counter
    if bin == keyshard_ID:
        updateVectorClock()
        event_counter = event_counter + 1
        event_log.append([copy.deepcopy(context[keyshard_ID]), 'PUT', keyname, req.get('value'), event_counter])
        if not req or "value" not in req:
            return jsonify(error='value is missing', message='Error in PUT'), 400

        # Check if key already exists
        if keyname in d:
            d[keyname]['value'] = req.get('value')
            d[keyname]['context'] = copy.deepcopy(context[keyshard_ID])
            return jsonify(message='Updated successfully', replaced=True), 200
        # Add new key
        else:
            d[keyname] = {}
            d[keyname]['value'] = req.get('value')
            d[keyname]['context'] = copy.deepcopy(context[keyshard_ID])
            return jsonify(message='Added successfully', replaced=False), 200

    else:
        return forward_request(request, view[bin])



# Get key
@app.route('/kv-store/keys/<keyname>', methods=['GET'])
def getKey(keyname):
    bin = hash(keyname) % len(view)
    isUpdated = True
    
    # Check if key already exists. If it does, find if the current context is the most updated one.
    if keyname in d:
        # Find if current context is the most updated one
        for entry in log:
            if entry[2] in d.keys():
                tempContext = d[entry[2]]['context']
            else:
                tempContext = [0 for i in context[keyshard_ID]]
                
            if areContextLarger(entry[0], tempContext):
                # gossip maybe?
                continue
            elif areContextConcurrent(entry[0], tempContext):
                continue
            else
                isUpdated = False
                break
        
        # If it is the most updated context we can return the most updated value
        # If it is not the most updated context we return a NACK
        if isUpdated is True:
            payload = {"doesExist": True, "message": 'Retrieved successfully', "value": d[keyname]['value']}
            # If it's not directly from client, add the correct address
            if 'from_node' in request.headers:
                payload['address'] = ADDRESS
            updateVectorClock()
            return jsonify(payload), 200
        else:
            return jsonify(message= 'NACK. Value is not current.')
            
    else:
        if 'from_node' in request.headers:
            return jsonify(doesExist=False, error='Key does not exist', message='Error in GET'), 404
        # otherwise forward it to the right node
        else:
            return forward_request(request, view[bin])


# Get shard (replicas not yet implemented)
@app.route('/kv-store/shards/<id>', methods=['GET'])
def getShard(id):
    bin = int(id)
    if bin < 0 or bin >= len(view):
        return jsonify({"message": "Node does not exist"})

    if view[bin] == ADDRESS:
        return jsonify({"message": 'Shard information retrieved successfully', "shard-id": bin, "key-count": len(d), "causal-context": '{}', "replicas": '{}'})
    else:
        return forward_request(request, view[bin])


# Get all shards
@app.route('/kv-store/shards', methods=['GET'])
def getAllShards():
    allShards = {}
    for node in view:
        allShards[node_ID] = len(d)
    return jsonify(allShards), 200
     
# Delete key
@app.route('/kv-store/keys/<keyname>', methods=['DELETE'])
def deleteKey(keyname):
    bin = hash(keyname) % len(view)

    if keyname in d:
        del d[keyname]
        payload = {'doesExist': True, 'message': 'Deleted successfully'}
        if 'from_node':
            payload['address'] = ADDRESS
        return jsonify(payload), 200
    else:
        if 'from_node' in request.headers:  # just need to check if there is a from_node header
            return jsonify(doesExist=False, error='Key does not exist', message='Error in DELETE'), 404
        else:
            return forward_request(request, view[bin])


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
# need to polish
@app.route('/gossip', methods=['PUT'])
def periodicGossipReceived():
    log = request.get_json()
    clock = context[keyshard_ID]
    counter = -1
    for entry in log:
        if entry[2] in d.keys():
            tempContext = d[entry[2]]['context']
        else:
            tempContext = [0 for i in context[keyshard_ID]]
        if areContextLarger(entry[0], tempContext):
            d[entry[2]] = {}
            d[entry[2]]['value'] = entry[3]
            d[entry[2]]['context'] = entry[0]
        elif areContextConcurrent(entry[0], tempContext):
            replace = False
            for index in range(len(entry[0])):
                if entry[0][index] > tempContext[index]:
                    replace = True
                    break
                elif entry[0][index] < tempContext[index]:
                    break
            if replace:
                d[entry[2]] = {}
                d[entry[2]]['value'] = entry[3]
                d[entry[2]]['context'] = entry[0]
        clock = entry[0]
        counter = entry[4]
    updateContext(clock)
    if counter > -1:
        requests.put(url="http://" + request.headers['from_node'] + "/ack/" + str(view.index(ADDRESS)),
                     headers={'from_node': ADDRESS, "Content-Type": "application/json"},
                     data=json.dumps({"counter": counter}))
    return ""

class Poop(dict):
    def __str__(self):
        return json.dumps(self, indent=4, sort_keys=True)

@app.route('/debug', methods=['GET'])
def debug():
        return "\nd\t" + str(Poop(d)) + "\nevent log\t" + str(event_log) + \
               "\ncontext\t" + str(context) + "\nacks\t" + \
               str(acks) + "\nkeyshard_ID\t" + str(keyshard_ID) + "\nnode_ID\t" + str(node_ID) + "\nMin\t" + \
               str(acks[min(acks, key=acks.get)]) + "\nevent_counter\t" + str(event_counter)


# acks of periodic gossip
# index is the index of the sender in view, because I'm lazy
@app.route('/ack/<index>', methods=['PUT'])
def ackReceived(index):
    acks[index] = request.get_json()['counter']
    minimum = acks[min(acks, key=acks.get)]
    while len(event_log) > 0 and event_log[0][4] <= minimum:
        event_log.pop(0)
    return ""


# Helper method to rehash and redistribute keys according to the new view
# Returns either an error message detailing which node failed to accept their new key(s) or the string "ok"
# This method tries to do everything in order, rather than broadcasting
@app.route('/kv-store/view-change', methods=['PUT'])
# perform a view change
def viewChange():
    global view
    req = request.get_json()
    new_view = req['view']
    view = new_view.split(',')
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
        for node in view:
            try:
                response = requests.get(url="http://" + node + "/kv-store/key-count")
                count = response.json()['key-count']
            except Exception:
                return "Node " + node + " did not respond to a request for its key count", 400
            view_map.append({"address": node, "key-count": count})
        return jsonify(message="View change successful", shards=view_map), 200
    else:
        return "ok", 200


# helper method to rehash and redistribute keys according to the new view
# returns either an error message detailing which node failed to accept their new key(s) or the string "ok"
# this method tries to do everything in order, rather than broadcasting
def key_distribute():
    for key in list(d.keys()):
        new_index = hash(key) % len(view)
        # If the key no longer belongs here, send it where it belongs
        if new_index != view.index(ADDRESS):
            try:
                requests.put(url="http://" + view[new_index] + "/kv-store/keys/" + key,
                             headers={'from_node': ADDRESS, "Content-Type": "application/json"},
                             data="{\"value\": \"" + d[key]['value'] + "\"}")
                del d[key]  # delete the key
            except Exception:
                return "Node " + view[new_index] + " did not accept key " + key
    return "ok"


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
        return jsonify(response.json()), response.status_code
    except ConnectionError:
        return jsonify(error='Node ' + node + " is down", message='Error in ' + request.method), 503
    except requests.exceptions.Timeout:
        return jsonify(error='Node ' + node + " is down", message='Error in ' + request.method), 503

@app.before_first_request
def before_first_request():
    scheduler = APScheduler()
    scheduler.init_app(app)
    app.apscheduler.add_job(func=periodicGossip, trigger='interval', seconds=10, id='0')
    scheduler.start()

def periodicGossip():
    temp = view.index(ADDRESS)
    if len(acks.keys()) < repl_factor - 1:
        for index in range(keyshard_ID, len(view), int(len(view) / repl_factor)):
            if index != temp:
                acks[str(index)] = -1
    for index in range(keyshard_ID, len(view), int(len(view) / repl_factor)):
        if index != temp:
            try:
                requests.put(url="http://" + view[index] + "/gossip",
                headers={'from_node': ADDRESS, "Content-Type": "application/json"},
                data = json.dumps([entry for entry in event_log if entry[4] > acks[str(index)]]))
            except ConnectionError:
                pass
            except requests.exceptions.ConnectionError:
                pass


if __name__ == "__main__":
    app.debug = True
    ADDRESS = sys.argv[1]
    view = sys.argv[2].split(',')
    #repl_factor = int(sys.argv[3])
    keyshard_ID = int(view.index(ADDRESS) % (len(view) / repl_factor))  # initialized to its index for post @188
    node_ID = int(math.ceil((view.index(ADDRESS) + 1) / (len(view) / repl_factor)) - 1)
    context = initialize_context()
    app.run(host='0.0.0.0', port=13800)