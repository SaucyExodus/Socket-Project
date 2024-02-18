# peer.py
import socket
import sys

class Peer:
    def __init__(self, peername, ipv4addr, mport, pport, status):
        self.peername = peername
        self.ipv4addr = ipv4addr
        self.mport = mport
        self.pport = pport
        self.status = status


# Main function
def main():
    # First two arguments are <Server IP Address> & <Server Port>
    server_IP = sys.argv[1]
    server_port = int(sys.argv[2])

    # Create Socket
    client_sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

    # Send a message to server
    message = input("Input a message you want to send: ")
    client_sock.sendto(message.encode(), (server_IP, server_port))

    # Recieve a message from server
    sever_message, server_address = client_sock.recvfrom(1024)
    print(sever_message)

    # Close the clients socket
    client_sock.close()

if __name__ == "__main__":
    main()