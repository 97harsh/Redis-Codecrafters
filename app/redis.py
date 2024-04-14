from time import time_ns
from typing import Dict, List

from app.parse import RESPParser
from app.utils import current_milli_time

class Redis:
    CONFIG = b"config"
    ECHO = b"echo"
    GET = b"get"
    INFO = b"info"
    SET = b"set"
    PING = b"ping"
    PX = b"px"
    REPLICATION = b"replication"

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
        if "replicaof" in config:
            self.role="slave"
        else:
            self.role="master"

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
        return f"role:{self.role}"
