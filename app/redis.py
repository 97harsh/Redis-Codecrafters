from app.parse import RESPParser

class Redis:
    def __init__(self):
        self.memory = {}
    
    def set_memory(self, key, value):
        """
        Stores key value pair in memory 
        """
        self.memory[RESPParser.convert_to_string(key)] = RESPParser.convert_to_string(value)
        return
    
    def get_memory(self, key):
        """
        Retreives valu for key from memory
        """
        return self.memory.get(RESPParser.convert_to_string(key),None)