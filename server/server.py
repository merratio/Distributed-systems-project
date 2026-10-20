import socket
import threading

HOST = ""
PORT = 8000

connected = set()#list of connected nodes
clients_lock = threading.Lock() #lock to make set thread safe

def broadcast(message, sender_conn):
    with clients_lock:
        for client in connected:
            if client != sender_conn:
                try:
                    client.sendall(message.encode())
                except socket.error, BrokenPipeError:
                    connected.discard(client)
                    try:
                        client.close()
                    except Exception:
                        pass



def handleClients(client, addr):
    print(f"{addr} has connected to the server")
    
    with clients_lock:
        connected.add(client)

    while True:
        try:
            message = client.recv(1024)
            if not message:
                break
            broadcast(f"{addr}: {message}", client)
            
        except socket.error:
            break
        
    with clients_lock:
        connected.discard(client)
    client.close()
    print(f"Client {addr} has disconnected")

def server():


    with socket.socket(socket.AF_INET,socket.SOCK_STREAM) as sock:
        sock.bind((HOST, PORT))
        sock.listen()
        print(f"Server is listening on port {PORT}")
        
        try:
            while True:

                conn, addr = sock.accept()
                thread = threading.Thread(
                        target=handleClients,
                        args=(conn, addr))
                thread.start()
        #thread.join()

        except KeyboardInterrupt:
            print("\n Server is stopping")

if __name__ == "__main__":
    server()


