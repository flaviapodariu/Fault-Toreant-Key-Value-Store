import socket
import json
import os
import yaml

cwd = os.path.dirname(__file__)
config_path = os.path.join(cwd, "config.yml")
config = yaml.safe_load(open(config_path))

REPLICA_PORT = config["datastore"]["port"]
REPLICA_LOG_FILE = "db-replica.log"
DATA_STORE_FILE = "db-replica.txt"

MASTER_LOG_FILE = "master_log.txt"

def start_replica(host='0.0.0.0', port=REPLICA_PORT):
    replica_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    replica_socket.bind((host, port))
    replica_socket.listen(5)
    print(f"Replica node started on {host}:{port}")

    recover_operations()

    while True:
        master_socket, addr = replica_socket.accept()
        print(f"Replication connection from {addr}")
        data = master_socket.recv(1024)
        if not data:
            master_socket.close()
            return
        
        operation = json.loads(data.decode('utf-8'))
        handle_missed(operation)
        

def recover_operations():
    if os.path.exists(MASTER_LOG_FILE):
        with open(MASTER_LOG_FILE, 'r') as log:
            for line in log:
                operation = json.loads(line.strip())
                handle_missed(operation)

def handle_missed(operation):
    if operation['action'] == 'INSERT':
        key = operation['key']
        value = operation['value']
        store_data(key, value)
        log_operation(operation)

def store_data(key, value):
    with open(DATA_STORE_FILE, 'a') as f:
        f.write(f"{key}:{value}\n")

def log_operation(request):
    with open(REPLICA_LOG_FILE, 'a') as log:
        log.write(json.dumps(request) + '\n')

if __name__ == '__main__':
    start_replica()
