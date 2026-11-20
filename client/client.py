# Distributed Systems - Client Development
# Rhian Sang - 2203004
# Part 2

import socket
import threading
from datetime import datetime

HOST = '127.0.0.1'  # server address when using the same computer
PORT = 8000         # same port used by the server


def receive_messages(client):
    # This keeps checking for messages from the server.
    while True:
        try:
            message = client.recv(1024)
            if not message:
                print('\nServer disconnected.')
                break
            print('\n' + message.decode('utf-8', errors='replace'))
        except OSError:
            break
    print('Connection closed. Press Enter to finish.')


def main():
    print('Welcome to our group chat!')
    name = input('Enter your name: ').strip() or 'Student'

    # Make a connection to the server.
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client.connect((HOST, PORT))
    except OSError:
        print('Could not connect. Make sure the server is running.')
        client.close()
        return

    print('Connected! Type /quit when you want to leave.')

    # Receive messages while I can still type and send my own.
    thread = threading.Thread(target=receive_messages, args=(client,), daemon=True)
    thread.start()

    try:
        while thread.is_alive():
            message = input()
            if message.strip().lower() == '/quit':
                break
            if not message.strip():
                continue

            # Add the name and time before sending the message.
            time = datetime.now().strftime('%H:%M:%S')
            text = f'[{time}] {name}: {message}'
            try:
                client.sendall(text.encode('utf-8'))
            except OSError:
                print('Message could not be sent. Connection closed.')
                break
    except (KeyboardInterrupt, EOFError):
        print('\nLeaving chat...')
    finally:
        client.close()
        print('You have left the chat.')


if __name__ == '__main__':
    main()
