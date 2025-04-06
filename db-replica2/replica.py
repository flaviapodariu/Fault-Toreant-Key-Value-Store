import socket
import json
import os
import yaml
import logging 

cwd = os.path.dirname(__file__)
config_path = os.path.join(cwd, "config.yml")
config = yaml.safe_load(open(config_path))

REPLICA_IP = config["datastore"]["ip"]
REPLICA_PORT = config["datastore"]["port"]

REPLICA_LOG_FILE = "db-replica.log"
DATA_STORE_FILE = "db-replica.txt"

MASTER_NODE_IP = config["master"]["ip"]
MASTER_NODE_PORT = config["master"]["port"]
MASTER_LOG_FILE = os.path.join(cwd, "../db-master/db-master.log")


def start_replica(host=REPLICA_IP, port=REPLICA_PORT):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))
    s.listen(5)
    print(f"Replica node started on {host}:{port}")

    # recover_operations(s.accept()[0])

    while True:
        replica_socket, addr = s.accept()
        print(f"Replication connection from {addr}")
        data = replica_socket.recv(1024)
        if not data:
            replica_socket.close()
            return
        
        operation = json.loads(data.decode('utf-8'))
        handle_incoming(operation)
        
def get_consistent_logs(socket):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((MASTER_NODE_IP, MASTER_NODE_PORT))
            message = json.dumps({"action": "LOGS", "internal_port":REPLICA_PORT})
            s.sendall(message.encode())
    except Exception as e:
        print("Could not get consistent logs from master")
        logging.error(e)
        raise Exception("Could not get consistent state from master node...")
    return wait_consistent_logs(socket)
    
def wait_consistent_logs(socket):
    while True:
        master_logs = socket.recv(1024)
        if not master_logs:
            socket.close()
            return
        print(f"Fetched recovery logs: {master_logs}")
        return json.loads(master_logs.decode("utf-8"))


def recover_operations(socket):
    logs = get_consistent_logs(socket)
    print(logs)
    # if os.path.exists(MASTER_LOG_FILE):
    #     with open(MASTER_LOG_FILE, 'r') as log:
    #         for line in log:
    #             operation = json.loads(line.strip())
    #             handle_incoming(operation)

def handle_incoming(operation):
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
