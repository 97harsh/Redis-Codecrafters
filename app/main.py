# Uncomment this to pass the first stage
import socket
import threading

from app.parse import RESPParser
from app.redis import Redis
from app.utils import convert_to_int
# from _thread import start_new_thread


def threaded(c, redis_object):
    # Function runs a thread for a connection
    while True:
        data = c.recv(1024)
        if not data:
            break
        data = RESPParser.process(data)
        data = redis_object.parse_arguments(data)
        if Redis.PING in data:
            c.send(RESPParser.convert_string_to_simple_string_resp(b"PONG"))
        elif Redis.ECHO in data:
            c.send(RESPParser.convert_string_to_bulk_string_resp(data[Redis.ECHO]))
        elif Redis.SET in data:
            redis_object.set_memory(data[Redis.SET][0],data[Redis.SET][1],data)
            c.send(RESPParser.convert_string_to_bulk_string_resp("OK"))
        elif Redis.GET in data:
            result = redis_object.get_memory(data[Redis.GET])
            if result is None:
                result = RESPParser.NULL_STRING
                c.send(result)
            else:
                c.send(RESPParser.convert_string_to_bulk_string_resp(result))
        elif Redis.CONFIG in data:
            config_data = data[Redis.CONFIG]
            if Redis.GET in config_data:
                result = redis_object.get_config(config_data[Redis.GET])
            if result is None:
                result = RESPParser.NULL_STRING
                c.send(result)
            else:
                c.send(RESPParser.convert_list_to_resp([config_data[Redis.GET],result]))
        elif Redis.INFO in data:
            info = redis_object.get_info()
            c.send(RESPParser.convert_string_to_bulk_string_resp(info))
        else:
            c.send(b"-Error message\r\n")
    c.close()

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
    while True:
        c, addr = sock.accept()
        print(f"Connected by {addr[0]}")
        t = threading.Thread(target=threaded, args=(c,redis_object))
        t.start()



if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', metavar='path', required=False, default=None)
    parser.add_argument('--dbfilename', metavar='str', required=False, default=None)
    parser.add_argument('--port', metavar='int', required=False, default=6379, type=int)
    parser.add_argument('--replicaof', nargs=2, metavar=('MASTER_HOST','MASTER_PORT'), required=False, default=None, type=(str,int))
    args = parser.parse_args()
    main(args)
