# Uncomment this to pass the first stage
import socket
import threading

from app.parse import RESPParser
from app.redis import Redis
from app.utils import convert_to_int
# from _thread import start_new_thread


# def threaded(c, redis_object):
#     # Function runs a thread for a connection
#     while True:
#         original_message = c.recv(1024)
#         if not data:
#             break
#         data = RESPParser.process(original_message)
#         data = redis_object.parse_arguments(data)
#         if Redis.PING in data:
#             c.send(RESPParser.convert_string_to_simple_string_resp(b"PONG"))
#         elif Redis.ECHO in data:
#             c.send(RESPParser.convert_string_to_bulk_string_resp(data[Redis.ECHO]))
#         elif Redis.SET in data:
#             redis_object.set_memory(data[Redis.SET][0],data[Redis.SET][1],data)
#             c.send(RESPParser.convert_string_to_bulk_string_resp("OK"))
#         elif Redis.GET in data:
#             result = redis_object.get_memory(data[Redis.GET])
#             if result is None:
#                 result = RESPParser.NULL_STRING
#                 c.send(result)
#             else:
#                 c.send(RESPParser.convert_string_to_bulk_string_resp(result))
#         elif Redis.CONFIG in data:
#             config_data = data[Redis.CONFIG]
#             if Redis.GET in config_data:
#                 result = redis_object.get_config(config_data[Redis.GET])
#             if result is None:
#                 result = RESPParser.NULL_STRING
#                 c.send(result)
#             else:
#                 c.send(RESPParser.convert_list_to_resp([config_data[Redis.GET],result]))
#         elif Redis.INFO in data:
#             info = redis_object.get_info()
#             c.send(RESPParser.convert_string_to_bulk_string_resp(info))
#         elif Redis.RELP_CONF in data:
#             c.send(RESPParser.convert_string_to_bulk_string_resp("OK"))
#         elif Redis.PSYNC in data:
#             c.send(RESPParser.convert_string_to_simple_string_resp(f"FULLRESYNC {redis_object.master_replid} {redis_object.master_repl_offset}"))
#             response = redis_object.send_rdb()
#             c.send(response)
#         else:
#             c.send(b"-Error message\r\n")
#         redis_object.queue.append(original_message)
#     c.close()

class RedisThread(threading.Thread):
    def __init__(self, conn, redis_object, do_handshake=False):
        super().__init__()
        self.redis_object = redis_object
        self.conn = conn
        self.talking_to_replica=False
        self.talk_to_master=do_handshake
        self.buffer_id = None

    def run(self):
        while True:
            if self.talking_to_replica:
                break
            original_message = self.conn.recv(1024)
            if not original_message:
                break
            data = RESPParser.process(original_message)
            data = self.redis_object.parse_arguments(data)
            if Redis.PING in data:
                self.conn.send(RESPParser.convert_string_to_simple_string_resp(b"PONG"))
            elif Redis.ECHO in data:
                self.conn.send(RESPParser.convert_string_to_bulk_string_resp(data[Redis.ECHO]))
            elif Redis.SET in data:
                self.redis_object.set_memory(data[Redis.SET][0],data[Redis.SET][1],data)
                if not self.talk_to_master:
                    self.conn.send(RESPParser.convert_string_to_bulk_string_resp("OK"))
            elif Redis.GET in data:
                result = self.redis_object.get_memory(data[Redis.GET])
                if result is None:
                    result = RESPParser.NULL_STRING
                    self.conn.send(result)
                else:
                    self.conn.send(RESPParser.convert_string_to_bulk_string_resp(result))
            elif Redis.CONFIG in data:
                config_data = data[Redis.CONFIG]
                if Redis.GET in config_data:
                    result = self.redis_object.get_config(config_data[Redis.GET])
                if result is None:
                    result = RESPParser.NULL_STRING
                    self.conn.send(result)
                else:
                    self.conn.send(RESPParser.convert_list_to_resp([config_data[Redis.GET],result]))
            elif Redis.INFO in data:
                info = self.redis_object.get_info()
                self.conn.send(RESPParser.convert_string_to_bulk_string_resp(info))
            elif Redis.RELP_CONF in data:
                self.conn.send(RESPParser.convert_string_to_bulk_string_resp("OK"))
            elif Redis.PSYNC in data:
                self.conn.send(RESPParser.convert_string_to_simple_string_resp(f"FULLRESYNC {self.redis_object.master_replid} {self.redis_object.master_repl_offset}"))
                response = self.redis_object.send_rdb()
                self.talking_to_replica=True # if the code reaches here, that means it is talking to the replica
                self.buffer_id = self.redis_object.add_new_replica()
                self.conn.send(response)
            else:
                self.conn.send(b"-Error message\r\n")
            if self.redis_object.replica_present and Redis.SET in data:
                self.redis_object.add_command_buffer(original_message)
        if self.talking_to_replica and self.redis_object.is_master():
            self.run_sync_replica()
        self.conn.close()

    def run_sync_replica(self):
        """
        This function checks if there is any new information in the queue and sends it to the replica server
        """
        while True:
            thread_queue = self.redis_object.buffers[self.buffer_id]
            if len(thread_queue)>0:
                command = thread_queue.popleft()
                self.conn.send(command)

class RedisMasterConnectThread(threading.Thread):
    def __init__(self, redis_object):
        """
        This is class that receives write infoo from master
        """
        super().__init__()
        self.redis_object = redis_object
        self.conn = None

    def run(self):
        self.conn = self.redis_object.do_handshake()
        while True:
            original_message = self.conn.recv(1024)
            if not original_message:
                break
            data = RESPParser.process(original_message)
            print("slave",data)
            data = self.redis_object.parse_arguments(data)
            if Redis.SET in data:
                print(f"setting {data[Redis.SET][0]}:{data[Redis.SET][1]}")
                self.redis_object.set_memory(data[Redis.SET][0],data[Redis.SET][1],data)
            else:
                self.conn.send(b"-Error message\r\n")
            if self.redis_object.replica_present and Redis.SET in data:
                self.redis_object.add_command_buffer(original_message)
        print("Closing Replica connection")
        self.conn.close()

def main(args):
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    redis_object = Redis(config=args)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("localhost", args.port))
    sock.listen()
    do_handshake=False
    if redis_object.role==Redis.SLAVE and not redis_object.already_connected_master:
        t = RedisMasterConnectThread(redis_object=redis_object)
        t.start()
    while True:
        c, addr = sock.accept()
        print(f"Connected by {addr[0]}")
        t = RedisThread(conn=c, redis_object=redis_object, do_handshake=do_handshake)
        # t = threading.Thread(target=threaded, args=(c,redis_object))
        t.start()



if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', metavar='path', required=False, default=None)
    parser.add_argument('--dbfilename', metavar='str', required=False, default=None)
    parser.add_argument('--port', metavar='int', required=False, default=6379, type=int)
    parser.add_argument('--replicaof', nargs=2, metavar=('MASTER_HOST','MASTER_PORT'),
                        required=False, default=None)
    args = parser.parse_args()
    print(args)
    main(args)
