# manager.py
import socket
import sys
import random
from peer import Peer

# Globle data structure
peer_list = []
DHT_list = []

dht_set_up = False
manager_state = "IDLE"

class DHT:
    def __init__(self, peername, ipv4addr, pport, status):
        self.peername = peername
        self.ipv4addr = ipv4addr
        self.pport = pport
        self.status = status

# Register function
def register(peer_name, ipv4addr, mport, pport):
    # Check if peer name is alphabetic and is less than 15
    if not peer_name.isalpha() or len(peer_name) > 15:
        return "FAILURE! Peer name must be an alphabetic string of length at most 15 characters."

    # Check if register is unique
    for obj in peer_list:
        # Check if mport and pport are unique
        if mport in obj.mport or pport in obj.pport:
            return "FAILURE! Ports already in use."
        
        # Check if peername is already registered
        if peer_name in obj.peername:
            return "FAILURE! Peer name already registered."

    # Register new peer
    status = "Free"
    peer_list.append(Peer(peer_name, ipv4addr, mport, pport, status))
    return "SUCCESS! Peer registered successfully."

def setup_dht(peername, n, year):

    global dht_set_up, manager_state

    # Check if the manager is in an idle state
    if manager_state != "IDLE":
        return "FAILURE! Manager is not available to process setup-dht request."

    # Check if the peer name is registered
    if not any(peer.peername == peername for peer in peer_list):
        return "FAILURE! Peer name is not registered."
    
    # Check if n is less than 3
    if len(peer_list) < 3:
        return "FAILURE! n must be at least three."
    
    # Check if less than n users are registered with manager
    if len(peer_list) < int(n):
        return "FAILURE! Fewer than n users are registered with the manager."
    
    # Check if the DHT has already been set up
    if dht_set_up:
        return "FAILURE! A DHT has already been set up."

    # Select n-1 free users at random
    free_peers = [peer for peer in peer_list if peer.status == "Free"]
    selected_peers = random.sample(free_peers, int(n) - 1)

    # Set selected peers status to InDHT
    for peer in selected_peers:
        peer.status = "InDHT"

    # Set leader's status to Leader for peer list
    leader_peer = next((peer for peer in peer_list if peer.peername == peername), None)
    
    if leader_peer:
        leader_peer.status = "Leader"

        peer_list.remove(leader_peer)
        peer_list.insert(0, leader_peer)

    # Set InDHT status for other peers
    #for peer in peer_list:
    #    if peer.status == "Free":
    #       peer.status = "InDHT"

    # Create DHT objects for selected peers and add them to DHT_list
    for peer in peer_list:
        if peer.status == "InDHT":
            dht_peer = DHT(peer.peername, peer.ipv4addr, peer.pport, peer.status)
            DHT_list.append(dht_peer)
        elif peer.status == "Leader":
            dht_peer = DHT(peer.peername, peer.ipv4addr, peer.pport, peer.status)
            DHT_list.insert(0, dht_peer)
        else:
            return

    # List of n peers that together will construct the DHT
    dht_peers = [(peer.peername, peer.ipv4addr, peer.pport) for peer in peer_list]

    # Update manager status to WAITING_DHT_COMPLETE
    manager_state = "WAITING_DHT_COMPLETE"

    # Set dht_set_up to true and print list of peers
    dht_set_up = True
    print("\n", dht_peers)
    return "SUCCESS! DHT is set up."

def print_manager_status():
    
    global manager_state
    print(manager_state)
    return "SUCCESS! Manager Status was printed" 

# Execution of commands from peer (Switch Case)
def command_execution(command_name):
    command = command_name.split()
    
    if command[0] == "register":
        command_response = register(command[1], command[2], command[3], command[4])
    elif command[0] == "print_peer_list":
        command_response = print_peer_list()
    elif command[0] == "setup_dht":
        command_response = setup_dht(command[1], command[2], command[3])
    elif command[0] == "print_manager_status":
        command_response = print_manager_status()
    elif command[0] == "print_DHT_list":
        command_response = print_DHT_list()
    else:
        command_response = "FAILURE! Couldn't find command."
     
    return command_response

# Print Peer List Function
def print_peer_list():
    for obj in peer_list:
        print("\nPeer name: ", obj.peername, "\nIPv4 Address: ", obj.ipv4addr, "\nManager Port: ", obj.mport, 
              "\nPeer Port: ", obj.pport, "\nStatus: ", obj.status, "\n----------------------------")
        
    return "SUCCESS! List was printed"

# Print DHT List Function
def print_DHT_list():
    for obj in DHT_list:
        print("\nPeer name: ", obj.peername, "\nIPv4 Address: ", obj.ipv4addr,  
              "\nPeer Port: ", obj.pport, "\n----------------------------")
        
    return "SUCCESS! List was printed"

# Main function
def main():
    # First argument for server port
    server_port = int(sys.argv[1])
    print("Server: Port server is listening to: ", server_port)

    # Create socket
    server_sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

    # Bind socket to localhost with port argument
    server_sock.bind(('', server_port))

    # Infinite loop for receiving/sending messages 
    while True:
        data, client_address = server_sock.recvfrom(1024)
        message = data.decode()  # Decode the received data
        response = command_execution(message)  # Execute the command
        server_sock.sendto(response.encode(), client_address)

if __name__ == "__main__":
    main()

