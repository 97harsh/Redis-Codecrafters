# Uncomment this to pass the first stage
import socket
import threading
from app.parse import RESPParser
from app.redis import Redis
# from _thread import start_new_thread
redis_object = Redis()

def threaded(c):
    # Function gets the connection, checks if it got a ping, returns a pong to it
    while True:
        data = c.recv(1024)
        if not data:
            break
        data = RESPParser.process(data)
        if data[0]==b"ping":
            c.send(RESPParser.convert_string_to_resp(b"PONG"))
        elif data[0]==b'echo':
            c.send(RESPParser.convert_string_to_resp(data[1]))
        elif data[0]==b'set':
            redis_object.set_memory(data[1],data[2])
            c.send(RESPParser.convert_string_to_resp("OK"))
        elif data[1]==b'get':
            result = redis_object.get_memory(data[1])
            c.send(RESPParser.convert_string_to_resp(result))
        else:
            c.send(b"-Error message\r\n")
    c.close()

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("localhost", 6379))
    sock.listen()
    while True:
        c, addr = sock.accept()
        print(f"Connected by {addr[0]}")
        t = threading.Thread(target=threaded, args=(c,))
        t.start()



if __name__ == "__main__":
    main()
