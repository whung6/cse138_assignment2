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

# Node's address
ADDRESS = ""

# Current view
view = []

# Insert and update key
@app.route('/kv-store/keys/<keyname>', methods = ['PUT'])
def putKey(keyname):
    
    bin = hash(keyname) % len(view) 
    # Check if keyname over 50 characters
    if len(keyname) > 50:
        return jsonify(error= 'Key is too long ', message= 'Error in PUT'), 201

    # Get request
    req = request.get_json()
    
    if(view[bin] == ADDRESS):
        # Check if key already exists
        if keyname in d:
            d[keyname] = req.get('value')
            return jsonify(message = 'Updated successfully', replaced = True), 200
        
    # Add new key
        if req:
            d[keyname] = d.get(keyname, {})
            d[keyname]['value'] = req.get('value')
            return jsonify(message = 'Added successfully',replaced = False), 201
        else:
            return jsonify(error = 'Value is missing', message = 'Error in PUT'), 400
    else:
        forward_request(req, view[bin])
        
# Get key    
@app.route('/kv-store/keys/<keyname>', methods = ['GET'])
def getKey(keyname):
    
    bin = hash(keyname) % len(view) 
    # Check if key already exists
    if keyname in d:
        payload = { "doesExist": True, "message": 'Retrieved successfully', "value": d[keyname]['value'] }
        # If it's not diredcly from client, add the correct address
        if 'from_node' in request.headers:
            payload['address'] = ADDRESS
        return jsonify(payload), 200
    else:
        if 'from_node' in request.headers: 
            return jsonify(doesExist = False, error = 'Key does not exist', message = 'Error in GET'), 404
        # Forward it to the right node
        else:
            return forward_request(request, view[bin])

# Delete key    
@app.route('/kv-store/keys/<keyname>', methods = ['DELETE'])
def deleteKey(keyname):
    
    bin = hash(keyname) % len(view) 
    
    if keyname in d:
        del d[keyname]
        payload = { 'doesExist': True, 'message': 'Deleted successfully' }
        if 'from_node':
            payload['address'] = ADDRESS
        return jsonify(payload), 200
    else:
        if 'from_node' in request.headers: #just need to check if there is a from_node header
            return jsonify(doesExist= False, error= 'Key does not exist', message = 'Error in DELETE'), 404
        else:
            return forward_request(request, view[bin])

# Get key count
@app.route('/kv-store/key-count', methods = ['GET'])
def getKeyCount():
    name = 'key-count'
    count = len(d)
    return jsonify(message = "Key count retrieved successfully", name = count), 200


@app.route('/kv-store/view-change',methods = ['PUT'])
#perform a view change
def viewChange():
    global view
    req = request.get_json()
    new_view = req['view']
    # If we need to, notify all the other nodes of this view change
    if 'from_node' not in request.headers:
        for node in iter(view):
            try:
                requests.put(node + "/kv-store/view-change", headers = {'from_node': ADDRESS}, data = request.get_data())
            except Exception:
                return "node " + node + " did not respond to notification of view change"
    #parse the new view list
    view = new_view.split(',')
    #do the reshard
    err = key_distribute()
    if err != "ok":
        return jsonify(message = "Error in PUT", error = err)
    else:
        view_map = []
        for node in iter(view):
            try:
                count = requests.get(node + "/kv-store/key-count")
            except Exception:
                return "node " + node + " did not respond to a request for its key count"
            view_map.append({address: node, key-count: count})
        return jsonify(message = "View change successful", shards = view_map)


# Helper method to rehash and redistribute keys according to the new view
# Returns either an error message detailing which node failed to accept their new key(s) or the string "ok"
# This method tries to do everything in order, rather than broadcasting

def key_distribute():
    for key in iter(d):
        new_index = hash(key) % len(view) 
        # If the key no longer belongs here, send it where it belongs
        if new_index != view.index(ADDRESS): 
            try:
                requests.put(view[new_index] + "/kv-store/keys/" + key, headers = {'from_node': ADDRESS}, data = jsonify({value: d[key]}))
                del d[key] # Delete the key
            except Exception:
                return "node " + view[new_index] + " did not accept key " + key
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
            url = request.url.replace(request.host, node),
            headers = headers,
            data = request.get_data(),
            timeout = 20)
        return jsonify(response.json()), response.status_code
    except Exception:
        return jsonify(error = 'Main instance is down', message= 'Error in ' + request.method), 503

if __name__ == "__main__":
    app.debug = True
    ADDRESS = sys.argv[1]
    view = sys.argv[2].split(',')
    app.run(host = '0.0.0.0', port = 13800)