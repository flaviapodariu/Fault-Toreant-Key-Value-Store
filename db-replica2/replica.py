import socket
import json
import os
import yaml
import logging 
import ast

cwd = os.path.dirname(__file__)
config_path = os.path.join(cwd, "config.yml")
config = yaml.safe_load(open(config_path))

REPLICA_IP = config["datastore"]["ip"]
REPLICA_PORT = config["datastore"]["port"]

REPLICA_LOG_FILE = "db-replica.log"
DATA_STORE_FILE = "db-replica.txt"

MASTER_NODE_IP = config["master"]["ip"]
MASTER_NODE_PORT = config["master"]["port"]

def start_replica(host=REPLICA_IP, port=REPLICA_PORT):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))
    s.listen(5)
    print(f"Replica node started on {host}:{port}")
    
    recover_operations(s)

    while True:
        replica_socket, addr = s.accept()
        print(f"Replication connection from {addr}")
        data = replica_socket.recv(1024)
        if not data:
            replica_socket.close()
            return
        
        operation = json.loads(data.decode('utf-8'))
        handle_incoming(operation)
        
def get_consistent_logs(socket_master):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((MASTER_NODE_IP, MASTER_NODE_PORT))
            message = json.dumps({"action": "LOGS", "internal_port":REPLICA_PORT})
            s.sendall(message.encode())
            master_conn =socket_master.accept()[0]
    except Exception as e:
        print("Could not get consistent logs from master")
        logging.error(e)
        raise Exception("Could not get consistent state from master node...")
    return wait_consistent_logs(master_conn)
    
def wait_consistent_logs(master_conn):
    while True:
        master_logs = master_conn.recv(3000)
        if not master_logs:
            return
        print(f"Fetched recovery logs: {master_logs}")
        return master_logs.decode("unicode_escape").strip("\"").strip().split("\n")


def recover_operations(s):
    # list of logs
    master_logs = get_consistent_logs(s)
    
    if master_logs == ['']:
        return
    
    last_log = dict()
    last_uuid = None

    if os.path.exists(REPLICA_LOG_FILE):
        replica_logs = open(REPLICA_LOG_FILE, 'r').readlines()
        last_log = ast.literal_eval(replica_logs[-1].strip()) if replica_logs else None
        last_uuid = last_log["uuid"] if last_log else None

    found_last = False if last_log else True

    for log in master_logs:
        print(log)
        operation = dict(json.loads(log.strip()))
        if found_last:
            handle_incoming(operation)
        elif operation["uuid"] == last_uuid:
            found_last = True


def handle_incoming(operation):
    if operation['action'] == 'INSERT':
        key = operation['key']
        value = operation['value']
        store_data(key, value)
        print(f"Data succesfully stored")
        log_operation(operation)

def store_data(key, value):
    with open(DATA_STORE_FILE, 'a') as f:
        f.write(f"{key}:{value}\n")

def log_operation(request):
    with open(REPLICA_LOG_FILE, 'a') as log:
        log.write(json.dumps(request) + '\n')

if __name__ == '__main__':
    start_replica()
