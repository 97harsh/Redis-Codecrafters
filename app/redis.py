from time import time

from app.parse import RESPParser
from app.utils import current_milli_time

class Redis:
    def __init__(self):
        self.memory = {}
        self.timeout = {} # Stores, current time, timeout in ms
    
    def set_memory(self, key, value, **args):
        """
        Stores key value pair in memory 
        """
        key = RESPParser.convert_to_string(key)
        value = RESPParser.convert_to_string(value)
        if 'px' in args:
            self.timeout[key] = [current_milli_time(),args['px']]
        self.memory[key] = value
        return

    def get_memory(self, key):
        """
        Retreives valu for key from memory
        """
        key = RESPParser.convert_to_string(key)
        if key in self.timeout and self.is_timeout(key):
            del self.timeout[key], self.memory[key]

        return self.memory.get(key,None)

    def is_timeout(self, key):
        if key not in self.timeout:
            # If key has no timeout mentioned
            return False
        key_entered_time = self.timeout[key][0]
        key_life = self.timeout[key][1]
        if current_milli_time()-key_entered_time>key_life:
            return True
        return False