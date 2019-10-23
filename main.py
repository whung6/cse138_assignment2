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

# is this instance a main instance
main = False
FORWARDING_ADDRESS = ""
# Insert and update key
@app.route('/kv-store/<keyname>', methods = ['PUT'])
def putKey(keyname):
    if not main:
        return forward_request(request)

    # Check if keyname over 50 characters
    if len(keyname) > 50:
        return jsonify(error= 'Key is too long ', message= 'Error in PUT'), 201

    # Get request
    req = request.get_json()

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

# Get key    
@app.route('/kv-store/<keyname>', methods = ['GET'])
def getKey(keyname):
    if not main:
        return forward_request(request)

    # Check if key already exists
    if keyname in d['data']:
        return jsonify(doesExist= True, message= 'Retrieved successfully',  value=d['data'][keyname]['value']), 200
    else:
        return jsonify(doesExist= False, error= 'Key does not exist',message='Error in GET'),404

# Delete key    
@app.route('/kv-store/<keyname>', methods = ['DELETE'])
def deleteKey(keyname):
    if not main:
        return forward_request(request)

    if keyname in d['data']:
        # delete some stuff 
        del d['data'][keyname]
        return jsonify(doesExist= True, message= 'Deleted successfully'), 200
    else:
        return jsonify(doesExist= False, error= 'Key does not exist', message= 'Error in DELETE'), 404


def forward_request(request):
    try:
        response = requests.request(
            method=request.method,
            url=request.url.replace(request.host, FORWARDING_ADDRESS),
            headers={key: value for (key, value) in request.headers if key != 'Host'},
            data=request.get_data(),
            timeout=20)
        return  jsonify(response.json()), response.status_code
    except Exception:
        return jsonify(error = 'Main instance is down', message= 'Error in ' + request.method), 503

if __name__ == "__main__":
    app.debug = True
    FORWARDING_ADDRESS = sys.argv[1];
    main = FORWARDING_ADDRESS == "Empty";
    app.run(host = '0.0.0.0', port = 13800)