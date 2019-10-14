from flask import Flask
from flask import jsonify
from redis import Redis, RedisError
from flask import request
import os
import socket
import json

# Connect to Redis
redis = Redis(host="redis", db = 0, socket_connect_timeout = 2, socket_timeout = 2)

app = Flask(__name__)

@app.route("/")
def default():
    return "CSE 138 Lab 2."

# Store key
d = {}
d['data'] = {}

# Insert and update key
@app.route('/kv-store/<keyname>', methods = ['PUT'])
def putKey(keyname):
    
    # Check if keyname over 50 characters
    if len(keyname) > 50:
        return jsonify('error:' 'Key is too long, ' + 'message:' + 'Error in PUT'), 201
    
    # Get request
    req = request.get_json()
    
    # Check if key already exists
    if keyname in d['data']:
        d['data'][keyname]['replaced'] = True
        d['data'][keyname]['value'] = req.get('value')
        return jsonify('message: ' + 'Updated successfully, ' + 'replaced: ' + json.dumps(d['data'][keyname]['replaced'])), 200
    
 # Add new key
    if req:
        d['data'][keyname] = d['data'].get(keyname, {})
        d['data'][keyname]['value'] = req.get('value')
        d['data'][keyname]['replaced'] = False
        return jsonify('message: ' + 'Added successfully, ' + 'replaced: ' + json.dumps(d['data'][keyname]['replaced'])), 201
    else:
        return jsonify('error: ' + 'value is missing,' + ' message: ' + 'Error in PUT'), 400

# Get key    
@app.route('/kv-store/<keyname>', methods = ['GET'])
def getKey(keyname):
    # Check if key already exists
    if keyname in d['data']:
        return jsonify('doesExist: true, ' + 'message: Retrieved successfully, ' + "value: " + json.dumps(d['data'][keyname]['value'])), 200
    else:
        return jsonify('doesExist: False')

# Delete key    
@app.route('/kv-store/<keyname>', methods = ['DELETE'])
def deleteKey(keyname):
    if keyname in d['data']:
        # delete some stuff 
        return jsonify('doesExist: true, message: Deleted successfully'), 200
    else:
        return jsonify('doesExist: false, error: Key does not exist, message: Error in DELETE'), 404
if __name__ == "__main__":
    app.debug = True
    app.run(host = '0.0.0.0', port = 13800)