from flask import Flask
from flask import jsonify
from flask import request
import os
import socket
import json
import sys
# flask's request isn't for sending request to other sites
import requests
import math

app = Flask(__name__)

# Store key
d = {}

# Node's address
ADDRESS = ""

# the vector clock index in context, add 1 if used as ID
# len(view) % repl_factor
keyhard_ID = 0

# the column of this node in the vector clock
# math.floor((view.index(ADDRESS) + 1) / repl_factor) - 1
# need to set to 0 if repl_factor = 1
node_ID = 0

# causal context
# when giving context back to the client, only modify one keyshard
# leave the other keyshards untouched
context = []

# replica factor
repl_factor = 1

# Current view
view = []


# creates a 2D array of 0's with size [keyshards][repl_factor]
# keyshards = number of nodes / repl_factor = number of keyshards
def initialize_context():
    return [[0 for x in range(len(view) / repl_factor)] for y in repl_factor]


# is own context > than the compared context
def isOwnContextLarger(context2):
    for index in range(len(context2[keyshard_ID])):
        if context[keyshard_ID][index] < context2[keyshard_ID][index]:
            return False
    return True


def updateVectorClock():
    context[keyshard_ID][node_ID] = context[keyshard_ID][node_ID] + 1


# compare against own context
def isContextConcurrent(context2):
    has_smaller, has_larger = False, False
    for index in range(len(context2[keyshard_ID])):
        if context[keyshard_ID][index] < context2[keyshard_ID][index]:
            has_smaller = True
        elif context[keyshard_ID][index] > context2[keyshard_ID][index]:
            has_larger = True
    return has_smaller and has_larger
# if it's not larger and not concurrent, it's smaller


##EXPERIMENTAL FEATURE:
#using xor-distance rather than modulo to distribute keys
#this is a drop-in replacement. Simply replace every use of view[hash(key) % len(view)] with xordist_get_addr(key)
#advantages: resharading does not require as many keys to change location during a reshard
#advantages: lookup is O(n) in the number of nodes rather than constant-time... but for n < 10,000 this is still practically nothing
def xordist_get_addr(key):
    key_hash = hash(key)
    dist_min = hash(ADDRESS)^key_hash
    addr_min = ADDRESS
    for node in iter(view): #find the minimum of distances(measured with XOR) between the hash of the address and the hash of the key
        if hash(node)^key_hash < dist_min:
            dist_min = hash(node)^key_hash
            addr_min = node
    return addr_min


@app.route("/")
def default():
    return "CSE 138 Lab 2."


# Insert and update key
@app.route('/kv-store/keys/<keyname>', methods=['PUT'])
def putKey(keyname):
    bin = hash(keyname) % len(view)

    # Check if keyname over 50 characters
    if len(keyname) > 50:
        return jsonify(error='Key is too long ', message='Error in PUT'), 201
        
    # Get request
    req = request.get_json()
    
    if view[bin] == ADDRESS:
        if not req or "value" not in req:
            return jsonify(error='value is missing', message='Error in PUT'), 400

        # Check if key already exists
        if keyname in d:
            d[keyname]['value'] = req.get('value')
            return jsonify(message='Updated successfully', replaced=True), 200
        # Add new key
        else:
            d[keyname] = {}
            d[keyname]['value'] = req.get('value')
            return jsonify(message='Added successfully', replaced=False), 200
    else:
        return forward_request(request, view[bin])

# Get key    
@app.route('/kv-store/keys/<keyname>', methods=['GET'])
def getKey(keyname):
    bin = hash(keyname) % len(view) 
    # Check if key already exists

    if keyname in d:
        payload = {"doesExist": True, "message": 'Retrieved successfully', "value": d[keyname]['value']}
        # If it's not directly from client, add the correct address
        if 'from_node' in request.headers:
            payload['address'] = ADDRESS
        return jsonify(payload), 200
    else:
        if 'from_node' in request.headers:
            return jsonify(doesExist= False, error='Key does not exist', message='Error in GET'), 404
        # otherwise forward it to the right node
        else:
            return forward_request(request, view[bin])

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
    return jsonify(view),200

@app.route('/key-distribute', methods=['PUT'])
def startDistribution():
    return key_distribute(),200

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
        return "ok",200


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
                del d[key] # delete the key
            except Exception:
                return "Node " + view[new_index] + " did not accept key " + key
    return "ok"


##EXPERIMENTAL FEATURE
# does the same thing as the above method, but adapted for xordist
def xordist_key_distribute():
    for key in iter(d):
        new_addr = xordist_get_addr(key)
        if new_addr != ADDRESS: #if the key no longer belongs here, send it where it belongs
            try:
                requests.put(new_addr + "/kv-store/keys/" + key, headers={'from_node': ADDRESS}, data = jsonify({value: d[key]}))
                del d[key] #delete the key
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
    except Exception:
        return jsonify(error='Node ' + node + " is down", message='Error in ' + request.method), 503


if __name__ == "__main__":
    app.debug = True
    ADDRESS = sys.argv[1]
    view = sys.argv[2].split(',')
    keyshard_ID = view.index(ADDRESS)  # initialized to its index for post @188
    initialize_context()
    app.run(host='0.0.0.0', port=13800)
