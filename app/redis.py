from collections import deque
import socket
from time import time_ns
from typing import Dict, List

from app.parse import RESPParser
from app.utils import current_milli_time, convert_to_int

class Redis:
    CAPABILITY = b"capa"
    CONFIG = b"config"
    ECHO = b"echo"
    GET = b"get"
    MASTER = "master"
    INFO = b"info"
    LISTENING_PORT=b"listening-port"
    SET = b"set"
    SLAVE = "slave"
    PING = b"ping"
    PX = b"px"
    PSYNC = b"PSYNC"
    REPLICATION = b"replication"
    RELP_CONF = b"REPLCONF"

    LEN_CAPABILITY = 2
    LEN_CONFIG = 1
    LEN_ECHO = 2
    LEN_GET = 2
    LEN_INFO = 2
    LEN_LISTENING_PORT = 2
    LEN_REPL_CONF = 1
    LEN_SET = 3
    LEN_PING = 1
    LEN_PX = 2
    LEN_PSYNC = 3

    def __init__(self,config):
        self.memory = {}
        self.timeout = {} # Stores, current time, timeout in ms
        self.config=vars(config)
        if self.config["replicaof"]:
            self.role=Redis.SLAVE
        else:
            self.role=Redis.MASTER
        self.master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        self.master_repl_offset = 0
        self.buffers = {}
        self.replica_present = False
        self.already_connected_master = False

    def set_memory(self, set_vals, data):
        """
        Stores key value pair in memory 
        """
        for i,(key,value) in enumerate(set_vals):
            key = RESPParser.convert_to_string(key)
            value = RESPParser.convert_to_string(value)
            if Redis.PX in data:
                self.timeout[key] = [current_milli_time(),
                                    RESPParser.convert_to_int(data[Redis.PX][i])]
            self.memory[key] = value
        return

    def get_memory(self, key):
        """
        Retreives value for key from memory
        """
        key = RESPParser.convert_to_string(key)
        if key in self.timeout and self.is_timeout(key):
            del self.timeout[key], self.memory[key]

        return self.memory.get(key,None)

    def is_timeout(self, key: str) -> bool:
        if key not in self.timeout:
            # If key has no timeout mentioned
            return False
        key_entered_time = self.timeout[key][0]
        key_life = self.timeout[key][1]
        current_time = current_milli_time()
        if current_time-key_entered_time>key_life:
            return True
        return False

    def parse_arguments(self, input: List) -> Dict:
        """
        Expects a list of input which parses and coverts to a dictionary
        """
        curr = 0
        result={}
        while curr<len(input):
            if input[curr].lower()==Redis.PING:
                result[Redis.PING]=None
                curr+=Redis.LEN_PING
            elif input[curr].lower()==Redis.ECHO:
                result[Redis.ECHO] = input[curr+1]
                curr+=Redis.LEN_ECHO
            elif input[curr].lower()==Redis.SET:
                result[Redis.SET] = result.get(Redis.SET,[])+[[input[curr+1], input[curr+2]]]
                curr+=Redis.LEN_SET
            elif input[curr].lower()==Redis.GET:
                result[Redis.GET] = input[curr+1]
                curr+=Redis.LEN_GET
            elif input[curr].lower()==Redis.PX:
                result[Redis.PX] = result.get(Redis.PX,[]) + [input[curr+1]]
                curr+=Redis.LEN_PX
            elif input[curr].lower()==Redis.CONFIG:
                result[Redis.CONFIG] = {}
                curr+=Redis.LEN_CONFIG
                config_result = result[Redis.CONFIG]
                if input[curr].lower()==Redis.GET:
                    config_result[Redis.GET] = input[curr+1]
                    curr+=Redis.LEN_GET
            elif input[curr].lower()==Redis.INFO:
                result[Redis.INFO] = input[curr+1]
                curr+=Redis.LEN_INFO
            elif input[curr]==Redis.RELP_CONF:
                result[Redis.RELP_CONF] = {}
                repl_result = result[Redis.RELP_CONF]
                curr+=Redis.LEN_REPL_CONF
                if input[curr]==Redis.LISTENING_PORT:
                    repl_result[Redis.LISTENING_PORT] = input[curr+1]
                    curr+=Redis.LEN_LISTENING_PORT
                while curr<len(input) and input[curr]==Redis.CAPABILITY:
                    repl_result[Redis.CAPABILITY] = repl_result.get(Redis.CAPABILITY,[])+[input[curr+1]]
                    curr+=Redis.LEN_CAPABILITY
            elif input[curr]==Redis.PSYNC:
                result[Redis.PSYNC] = input[curr+1:]
                curr+=Redis.LEN_PSYNC
            else:
                # print(f"Unknown command {input[curr]}")
                pass
        return result

    def get_config(self, key):
        """
        Retreives config related information
        """
        key = RESPParser.convert_to_string(key)
        return self.config.get(key,None)
    
    def get_info(self, key=None):
        info = [f"role:{self.role}"]
        info.append(f"master_replid:{self.master_repl_offset}")
        info.append(f"master_repl_offset:{self.master_repl_offset}")
        return "\n".join(info)

    def do_handshake(self):
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        MasterHostname = self.config['replicaof'][0]
        MasterPort = convert_to_int(self.config['replicaof'][1])
        client_sock.connect((MasterHostname,MasterPort))
        client_sock.send(RESPParser.convert_list_to_resp(["ping"]))
        pong = client_sock.recv(1024)
        print(RESPParser.process(pong))
        response = [Redis.RELP_CONF,"listening-port",self.config["port"]]
        client_sock.send(RESPParser.convert_list_to_resp(response))
        pong = client_sock.recv(1024)
        response = [Redis.RELP_CONF, "capa", "eof", "capa", "psync2"]
        client_sock.send(RESPParser.convert_list_to_resp(response))
        pong = client_sock.recv(1024)
        response = [Redis.PSYNC, "?","-1"]
        client_sock.send(RESPParser.convert_list_to_resp(response))
        pong = client_sock.recv(1024)
        # client_sock.close()
        return client_sock

    def send_rdb(self):
        file_content="524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        file_content = bytes.fromhex(file_content)
        len_file = len(file_content)
        response = b"$"+RESPParser.convert_to_binary(len_file)+b"\r\n"+file_content
        return response

    def is_master(self):
        """
        Returns true is this server instance is the master server
        """
        return self.role==Redis.MASTER

    def add_command_buffer(self, command):
        """
        This function adds this command to all the buffers talking to the replicas
        """
        for k,_ in self.buffers.items():
            self.buffers[k].append(command)
        return 0

    def add_new_replica(self, ):
        """
        This function takes care of everything needed to add a new replica
        1. Create a new buffer
        Returns the ID of the buffer to use
        """
        self.replica_present = True
        Id = len(self.buffers)
        self.buffers[Id] = deque([])
        return Id