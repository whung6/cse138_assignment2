from flask import Flask
from flask import jsonify
from redis import Redis, RedisError
from flask import request
import os
import socket
import json
import sys
# flask's request isn't for sending request to other sites
import requests

# Connect to Redis
redis = Redis(host="redis", db = 0, socket_connect_timeout = 2, socket_timeout = 2)

app = Flask(__name__)

@app.route("/")
def default():
    return "CSE 138 Lab 2."

# Store key
d = {}
d['data'] = {}

#this nodes' address
ADDRESS = ""
#current view
view=[]


# Insert and update key
@app.route('/kv-store/keys/<keyname>', methods = ['PUT'])
def putKey(keyname):
    # Check if keyname over 50 characters

    bin = hash(keyname) % len(view)-1


    if len(keyname) > 50:
        return jsonify(error= 'Key is too long ', message= 'Error in PUT'), 201

        
    # Get request
    req = request.get_json()
    
    if(view[bin]==ADDRESS):
        # Check if key already exists
        if keyname in d['data']:
            d['data'][keyname]['value'] = req.get('value')
            return jsonify(message =  'Updated successfully',replaced=True), 200
 # Add new key
        if req:
            d['data'][keyname] = d['data'].get(keyname, {})
            d['data'][keyname]['value'] = req.get('value')
            return jsonify(message = 'Added successfully',replaced=False), 201
        else:
            return jsonify(error = 'value is missing', message = 'Error in PUT'), 400
    else:
        forward_request(req, view[bin])

# Get key    
@app.route('/kv-store/keys/<keyname>', methods = ['GET'])
def getKey(keyname):
    # Check if key already exists
    if keyname in d['data']:
        payload = { "doesExist": True, "message": 'Retrieved successfully', "value": d['data'][keyname]['value'] }
        #if it's not diredtly from client, add the correct address
        if 'from_node' in request.headers:
            payload['address'] = ADDRESS
        return jsonify(payload), 200
    else:
        if 'from_node' in request.headers #always fail if this request was forwarded. Only want one forward to happen.
            return jsonify(doesExist= False, error= 'Key does not exist',message='Error in GET'), 404
        #otherwise forward it to the right node
        else:
            return forward_request(request,view[hash(keyname) % len(view)])

# Delete key    
@app.route('/kv-store/keys/<keyname>', methods = ['DELETE'])
def deleteKey(keyname):

    #same things as get
    if keyname in d['data']:
        # delete some stuff 
        del d['data'][keyname]
        payload = { 'doesExist': True, 'message': 'Deleted successfully' }
        if from_node:
            payload['address'] = ADDRESS
        return jsonify(payload), 200
    else:
        if 'from_node' in request.headers #just need to check if there is a from_node header
            return jsonify(doesExist= False, error= 'Key does not exist', message= 'Error in DELETE'), 404
        else:
            return forward_request(request,view[hash(keyname) % len(view)])

#helper method to rehash and redistribute keys according to the new view
#returns either an error message detailing which node failed to accept their new key(s) or the string "ok"
#this method tries to do everything in order, rather than broadcasting
def key_distribute():
    for key in iter(d):
        new_index = hash(key) % len(view)
        if new_index != view.index(ADDRESS): #if the key no longer belongs here, send it where it belongs
            try:
                requests.put(view[new_index] + "/kv-store/keys/" + key, headers={'from_node': ADDRESS}, data=jsonify(value = d[key]))
            except Exception:
                "node " + view[new_index] " did not accept key " + key


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
    #get the next address if we need to forward, wrap around the list
    next_address = view[(view.index(ADDRESS) + 1) % len(view)]
    app.run(host = '0.0.0.0', port = 13800)
