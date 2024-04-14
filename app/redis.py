import socket
from time import time_ns
from typing import Dict, List

from app.parse import RESPParser
from app.utils import current_milli_time, convert_to_int

class Redis:
    CONFIG = b"config"
    ECHO = b"echo"
    GET = b"get"
    INFO = b"info"
    SET = b"set"
    PING = b"ping"
    PX = b"px"
    REPLICATION = b"replication"
    RELP_CONF = b"REPLCONF"

    LEN_CONFIG = 1
    LEN_ECHO = 2
    LEN_GET = 2
    LEN_INFO = 2
    LEN_SET = 3
    LEN_PING = 1
    LEN_PX = 2

    def __init__(self,config):
        self.memory = {}
        self.timeout = {} # Stores, current time, timeout in ms
        self.config=vars(config)
        if self.config["replicaof"]:
            self.role="slave"
        else:
            self.role="master"
        self.master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        self.master_repl_offset = 0

    def set_memory(self, key, value, data):
        """
        Stores key value pair in memory 
        """
        key = RESPParser.convert_to_string(key)
        value = RESPParser.convert_to_string(value)
        if Redis.PX in data:
            self.timeout[key] = [current_milli_time(),
                                 RESPParser.convert_to_int(data[Redis.PX])]
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
                result[Redis.SET] = [input[curr+1], input[curr+2]]
                curr+=Redis.LEN_SET
            elif input[curr].lower()==Redis.GET:
                result[Redis.GET] = input[curr+1]
                curr+=Redis.LEN_GET
            elif input[curr].lower()==Redis.PX:
                result[Redis.PX] = input[curr+1]
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
            else:
                raise ValueError(f"Unknown command {input[curr]}")
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

    def do_handshake(self,):
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        MasterHostname = self.config['replicaof'][0]
        MasterPort = convert_to_int(self.config['replicaof'][1])
        client_sock.connect((MasterHostname,MasterPort))
        client_sock.send(RESPParser.convert_list_to_resp(["ping"]))
        pong = client_sock.recv(1024)
        print(RESPParser.process(pong))
        # if RESPParser.process(pong)=="PONG":
        response = [Redis.RELP_CONF,"listening-port",self.config["port"]]
        client_sock.send(RESPParser.convert_list_to_resp(response))
        pong = client_sock.recv(1024)
        response = [Redis.RELP_CONF, "capa psync2"]
        client_sock.send(RESPParser.convert_list_to_resp(response))
        pong = client_sock.recv(1024)
        client_sock.close()
        return