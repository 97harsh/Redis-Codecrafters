# Uncomment this to pass the first stage
import socket


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    conn, addr = server_socket.accept() # wait for client
    with conn:
        print(f"Connected by {addr}")
        while True:
            data = conn.recv(1024)
            # print(data)
            if b"ping\r\n" in data:
            # if data==b"*1\r\n$4\r\nping\r\n":
                conn.sendall(b"+PONG\r\n")
            else:
                conn.sendall(b"-Error message\r\n")
    


if __name__ == "__main__":
    main()
