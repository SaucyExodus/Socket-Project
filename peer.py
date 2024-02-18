# peer.py
import socket
import sys
import pickle
import threading

peers_tuple = []

class Peer:
    def __init__(self, peername, ipv4addr, mport, pport, status, identifier, neighbor):
        self.peername = peername
        self.ipv4addr = ipv4addr
        self.mport = mport
        self.pport = pport
        self.status = status
        self.identifier = identifier
        self.neighbor = neighbor

    def set_identifier(self, identifier):
        self.identifier = identifier

    def set_right_neighbor(self, neighbor):
        self.right_neighbor = neighbor

def receive_messages(client_sock):
    while True:
        try:
            data, _ = client_sock.recvfrom(1024)
            peers_list = pickle.loads(data)
            print("Establishing ring connection:",  peers_list, "\n")
            # Process peers_list as needed
        except pickle.UnpicklingError:
            server_message = data.decode("utf-8")
            print("Received message:", server_message, "\n")

# Main function
def main():
    # First two arguments are <Server IP Address> & <Server Port>
    server_IP = sys.argv[1]
    server_port = int(sys.argv[2])

    # Create Socket
    client_sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    client_sock.sendto(b"Hello", (server_IP, server_port))

    # Start a separate thread for receiving messages
    receive_thread = threading.Thread(target=receive_messages, args=(client_sock,))
    receive_thread.start()

    while True:
             
        # Send a message to server
        message = input()
        client_sock.sendto(message.encode("utf-8"), (server_IP, server_port))

if __name__ == "__main__":
    main()