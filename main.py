from flask import Flask
from flask import jsonify
from flask import request
import os
import socket
import json
import sys
# flask's request isn't for sending request to other sites
import requests

app = Flask(__name__)

@app.route("/")
def default():
    return "CSE 138 Lab 2."

# Store key
d = {}

#this nodes' address
ADDRESS = ""
#current view
view=[]


##EXPERIMENTAL FEATURE:
#using xor-distance rather than modulo to distribute keys
#this is a drop-in replacement. Simply replace every use of view[hash(key) % len(view)] with xordist_get_addr(key)
#advantages: resharading does not require as many keys to change location during common view change patterns(adding/removing a single node)
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


# Insert and update key
@app.route('/kv-store/keys/<keyname>', methods = ['PUT'])
def putKey(keyname):
    # Check if keyname over 50 characters
    if len(keyname) > 50:
        return jsonify(error= 'Key is too long ', message= 'Error in PUT'), 201

    # Get request
    req = request.get_json()

    # Check if key already exists
    if keyname in d:
        d[keyname]['value'] = req.get('value')
        return jsonify(message =  'Updated successfully',replaced=True), 200
    
 # Add new key
    if req:
        d[keyname] = d.get(keyname, {})
        d[keyname]['value'] = req.get('value')
        return jsonify(message = 'Added successfully',replaced=False), 201
    else:
        return jsonify(error = 'value is missing', message = 'Error in PUT'), 400

# Get key    
@app.route('/kv-store/keys/<keyname>', methods = ['GET'])
def getKey(keyname):
    # Check if key already exists
    if keyname in d:
        payload = { "doesExist": True, "message": 'Retrieved successfully', "value": d[keyname]['value'] }
        #if it's not diredtly from client, add the correct address
        if 'from_node' in request.headers:
            payload['address'] = ADDRESS
        return jsonify(payload), 200
    else:
        if 'from_node' in request.headers: #always fail if this request was forwarded. Only want one forward to happen.
            return jsonify(doesExist= False, error= 'Key does not exist',message='Error in GET'), 404
        #otherwise forward it to the right node
        else:
            return forward_request(request,view[hash(keyname) % len(view)])

# Delete key    
@app.route('/kv-store/keys/<keyname>', methods = ['DELETE'])
def deleteKey(keyname):

    #same things as get
    if keyname in d:
        # delete some stuff 
        del d[keyname]
        payload = { 'doesExist': True, 'message': 'Deleted successfully' }
        if from_node:
            payload['address'] = ADDRESS
        return jsonify(payload), 200
    else:
        if 'from_node' in request.headers: #just need to check if there is a from_node header
            return jsonify(doesExist= False, error= 'Key does not exist', message= 'Error in DELETE'), 404
        else:
            return forward_request(request,view[hash(keyname) % len(view)])

@app.route('/kv-store/key-count', methods=['GET'])
def getKeyCount():
    name = 'key-count'
    count = len(d)
    return jsonify(message="Key count retrieved successfully", name=count),200


@app.route('/kv-store/view-change',methods=['PUT'])
#perform a view change
def viewChange():
    req = request.get_json()
    new_view = req['view']
    #if we need to, notify all the other nodes of this view change
    if 'from_node' not in request.headers:
        for node in iter(view):
            try:
                requests.put(node + "/kv-store/view-change", headers={'from_node': ADDRESS}, data=request.get_data())
            except Exception:
                return "node " + node + "did not respond to notification of view change"
    #parse the new view list
    view = new_view.split(',')
    #do the reshard
    err = key_distribute()
    if err != "ok":
        return jsonify(message="Error in PUT",error=err)
    else:
        view_map = []
        for node in iter(view):
            try:
                count = requests.get(node + "/kv-store/key-count")
            except Exception:
                return "node " + node + " did not respond to a request for its key count"
            view_map.append({address: node, key-count: count})
        return jsonify(message="View change successful", shards = view_map)


#helper method to rehash and redistribute keys according to the new view
#returns either an error message detailing which node failed to accept their new key(s) or the string "ok"
#this method tries to do everything in order, rather than broadcasting
def key_distribute():
    for key in iter(d):
        new_index = hash(key) % len(view)
        if new_index != view.index(ADDRESS): #if the key no longer belongs here, send it where it belongs
            try:
                requests.put(view[new_index] + "/kv-store/keys/" + key, headers={'from_node': ADDRESS}, data = jsonify({value: d[key]}))
                del d[key] #delete the key
            except Exception:
                return "node " + view[new_index] + " did not accept key " + key
    return "ok"

##EXPERIMENTAL FEATURE
#does the same thing as the above method, but adapted for xordist
def xordist_key_distribute():
    for key in iter(d):
        new_addr = xordist_get_addr(key)
        if new_index != ADDRESS: #if the key no longer belongs here, send it where it belongs
            try:
                requests.put(new_addr + "/kv-store/keys/" + key, headers={'from_node': ADDRESS}, data = jsonify({value: d[key]}))
                del d[key] #delete the key
            except Exception:
                return "node " + new_addr + " did not accept key " + key
    return "ok"


#forwards a request to the given address
def forward_request(request,node):
    #get the headers since request is immutable
    headers = {key: value for (key, value) in request.headers}
    #if it's not from another node but needs to be forwarded
    if 'from_node' not in request.headers:
        #mark that this is forwarded from this node
        headers['from_node'] = ADDRESS
    try:
        response = requests.request(
            method=request.method,
            url=request.url.replace(request.host, node),
            headers=headers,
            data=request.get_data(),
            timeout=20)
        return  jsonify(response.json()), response.status_code
    except Exception:
        return jsonify(error = 'Main instance is down', message= 'Error in ' + request.method), 503

if __name__ == "__main__":
    app.debug = True
    ADDRESS = sys.argv[1]
    view = sys.argv[2].split(',')
    app.run(host = '0.0.0.0', port = 13800)
