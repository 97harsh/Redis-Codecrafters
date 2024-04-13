# Uncomment this to pass the first stage
import socket
import threading
# from _thread import start_new_thread
# print_lock = threading.Lock()

def threaded(c):
    # Function gets the connection, checks if it got a ping, returns a pong to it
    while True:
        data = c.recv(1024)
        if not data:
            break
        if data==b"*1\r\n$4\r\nping\r\n":
            c.send(b"+PONG\r\n")
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
        # print_lock.acquire()
        
        # start_new_thread(threaded, (c,))
    sock.close()
    # server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    # conn, addr = server_socket.accept() # wait for client
    # with conn:
    #     print(f"Connected by {addr}")
    #     while True:
    #         data = conn.recv(1024)
    #         # print(data)
    #         # if b"ping\r\n" in data:
    #         if data==b"*1\r\n$4\r\nping\r\n":
    #             conn.send(b"+PONG\r\n")
    #         else:
    #             conn.send(b"-Error message\r\n")
    


if __name__ == "__main__":
    main()
