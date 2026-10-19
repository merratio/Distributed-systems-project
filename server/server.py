import socket
import threading

HOST = ""
PORT = 8000

connected = [] #list of connected nodes

def readWrite(conn):
    with conn:
        connected.append(conn)
        hello()
        print('Connected')
        while True:
            data = conn.recv(1024)
            if not data:
                break

            for i in connected:
                if i != conn:
                    i.sendall(data)



def hello():
    count = 0
    for i in connected:
        print(count)
        count+=1

with socket.socket(socket.AF_INET,socket.SOCK_STREAM) as sock:
    sock.bind((HOST, PORT))

    while True:
        sock.listen()
        print('Server is running')

        conn, addr = sock.accept()
        thread = threading.Thread(target=readWrite, args=(conn,))
        thread.start()
        #thread.join()

