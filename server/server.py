from flask import Flask, request, jsonify
import socket
import json
import uuid

app = Flask(__name__)

MASTER_NODE_IP = "0.0.0.0"
MASTER_NODE_PORT = 5001

@app.route('/pair', methods=['POST'])
def set_key_value():
    try:
        data = request.json
        key = data['key']
        value = data['value']

        save(key, value)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    
    return jsonify({"message": "Key-value pair received and forwarded to master."}), 200


def save(key, value):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((MASTER_NODE_IP, MASTER_NODE_PORT))
            message = json.dumps({"uuid": str(uuid.uuid4()) ,"action": "INSERT", "key": key, "value": value})
            s.sendall(message.encode())
    except Exception as e:
        print(f"Error sending data to master: {e}")
        raise Exception("Could not save pair")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

