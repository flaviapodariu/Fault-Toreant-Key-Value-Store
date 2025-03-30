import socket
import json
import threading
import yaml
import os
from datetime import datetime

cwd = os.path.dirname(__file__)
config_path = os.path.join(cwd, "config.yml")
config = yaml.safe_load(open(config_path))

MASTER_NODE_PORT = config["master"]["port"]
REPLICA_NODES = [(replica["ip"], replica["port"]) for replica in config["replicas"]]

DATA_FILE = os.path.join(cwd,"db-master.txt")
LOG_FILE = os.path.join(cwd, "db-master.log")

def handle_client(client_socket):
    data = client_socket.recv(1024)
    if not data:
        client_socket.close()
        return

    operation = json.loads(data.decode('utf-8'))

    if operation['action'] == 'INSERT':
        key = operation['key']
        value = operation['value']

        store_data(key, value)

        log_operation(operation)

        replicate_to_replicas(operation)

    client_socket.close()

def store_data(key, value):
    with open(DATA_FILE, 'a') as f:
        f.write(f"{key}:{value}\n")

def log_operation(operation):
    operation["timestamp"] = datetime.now().isoformat()
    with open(LOG_FILE, 'a') as log:
        log.write(f"{json.dumps(operation)}\n")

def replicate_to_replicas(operation):
    for replica in REPLICA_NODES:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(replica)
                s.sendall(json.dumps(operation).encode())
        except Exception as e:
            print(f"Error replicating to replica {replica}: {e}")

def start_master(host='0.0.0.0', port=MASTER_NODE_PORT):
    master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    master.bind((host, port))
    master.listen(5)
    print(f"Master node started at {host}:{port}")

    while True:
        client_socket, addr = master.accept()
        print(f"Connection from {addr}")
        client_handler = threading.Thread(target=handle_client, args=(client_socket,))
        client_handler.start()

if __name__ == '__main__':
    start_master()
