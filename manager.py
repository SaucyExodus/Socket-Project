# manager.py
import socket
import csv
import sys
import pickle
import random
from peer import Peer

# Globle data structure
peer_list = []
DHT_list = []

dht_set_up = False
manager_state = "IDLE"
numOfStormEvents = 0

# DHT Class
class DHT:
    def __init__(self, peername, ipv4addr, pport, status, identifier, neighbor):
        self.peername = peername
        self.ipv4addr = ipv4addr
        self.pport = pport
        self.status = status
        self.identifier = identifier
        self.neighbor = neighbor
        self.local_hash_table = []

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
    identifier = -1
    neighbor = None
    peer_list.append(Peer(peer_name, ipv4addr, mport, pport, status, identifier, neighbor))
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
    if int(n) < 3:
        return "FAILURE! n must be at least three."
    
    # Check if less than n users are registered with manager
    if len(peer_list) < int(n):
        return "FAILURE! Fewer than n users are registered with the manager."
    
    # Check if the DHT has already been set up
    if dht_set_up:
        return "FAILURE! A DHT has already been set up."

     # Set leader's status to Leader for peer list
    leader_peer = next((peer for peer in peer_list if peer.peername == peername), None)
    
    if leader_peer:
        leader_peer.status = "Leader"

        peer_list.remove(leader_peer)
        peer_list.insert(0, leader_peer)

    # Select n-1 free users at random
    free_peers = [peer for peer in peer_list if peer.status == "Free"]
    
    # Exclude the leader from the list of free peers
    free_peers_except_leader = [peer for peer in free_peers if peer != leader_peer]

    # Select n-1 free users at random, excluding the leader
    selected_peers = random.sample(free_peers_except_leader, min(len(free_peers_except_leader), int(n) - 1))

    # Set selected peers status to InDHT
    for peer in selected_peers:
        if peer.status == "Free":
           peer.status = "InDHT"

    # Filter the list to include only leaders and peers with status "InDHT"
    eligible_peers = [peer for peer in peer_list if peer.status == "Leader" or peer.status == "InDHT"]

    # Assign identifiers to selected peers
    for i, peer in enumerate(eligible_peers):
        if peer.status == "InDHT":
            peer.set_identifier(i)
        elif peer.status == "Leader":
            peer.set_identifier(0)

    # Filter the list to include only leaders and peers with status "InDHT"
    eligible_peers = [peer for peer in peer_list if peer.status == "Leader" or peer.status == "InDHT"]

    # Assign neighbors
    for i, peer in enumerate(eligible_peers):
        next_index = (i + 1) % len(eligible_peers)
        peer.neighbor = eligible_peers[next_index]

    # Assign the leader as the neighbor of the last peer to close the loop
    last_peer = eligible_peers[-1]
    last_peer.neighbor = eligible_peers[0]  # Assign the leader as the neighbor

    # Create DHT objects for selected peers and add them to DHT_list
    for peer in peer_list:
        if peer.status == "InDHT":
            dht_peer = DHT(peer.peername, peer.ipv4addr, peer.pport, peer.status, peer.identifier, peer.neighbor)
            DHT_list.append(dht_peer)
        elif peer.status == "Leader":
            dht_peer = DHT(peer.peername, peer.ipv4addr, peer.pport, peer.status, peer.identifier, peer.neighbor)
            DHT_list.insert(0, dht_peer)
        

    # List of n peers that together will construct the DHT
    dht_peers = [(peer.peername, peer.ipv4addr, peer.pport) for peer in DHT_list]
    serialized_data = pickle.dumps(dht_peers)
    server_sock.sendto(serialized_data, client_address)

    # Load csv file depending on the inputted year
    rows = []
    global numOfStormEvents

    if year == "1950":
        # Open the CSV file for 1950
        with open("./details-1950.csv", 'r') as file:
            csvreader = csv.reader(file)
            header = next(csvreader)
            for row in csvreader:
                rows.append(row)
                hash_table(row, n)
                numOfStormEvents += 1
    elif year == "1951":
        # Open the CSV file for 1951
        with open("./details-1951.csv", 'r') as file:
            csvreader = csv.reader(file)
            header = next(csvreader)
            for row in csvreader:
                rows.append(row)
                hash_table(row, n)
                numOfStormEvents += 1
    elif year == "1952":
        # Open the CSV file for 1952
        with open("./details-1952.csv", 'r') as file:
            csvreader = csv.reader(file)
            header = next(csvreader)
            for row in csvreader:
                rows.append(row)
                hash_table(row, n)
                numOfStormEvents += 1
        #print("\n", header)
        #print("\n", rows[0:5])
        #print("\n", numOfStormEvents)

    # Update manager status to WAITING_DHT_COMPLETE
    manager_state = "WAITING_DHT_COMPLETE"

    # Set dht_set_up to true and print list of peers
    dht_set_up = True
    dht_peers_printed_list = ""
    for i in dht_peers:
        dht_peers_printed_list += f"\nPeer name: {i[0]} \nIPv4 Address: {i[1]} \nPeer Port: {i[2]} \n----------------------------"
    
    return f"SUCCESS! DHT is set up." + dht_peers_printed_list

def dht_complete(peername):
    
    global dht_set_up, manager_state

    # Check if the manager is waiting for dht-complete
    if manager_state != "WAITING_DHT_COMPLETE":
        return "FAILURE! Manager is not waiting for DHT-complete."

    if not dht_set_up:
        return "FAILURE! DHT has not been set up yet."
    
    # Check if the provided peer name matches the leader of the DHT
    leader_found = any(peer.peername == peername and peer.status == "Leader" for peer in peer_list)
    if not leader_found:
        return "FAILURE! The provided peer name is not the leader of the DHT."
    
    # Reset manager state to idle
    manager_state = "DHT Complete"

    # Respond with SUCCESS to the leader
    return "SUCCESS! DHT setup completed successfully."

def hash_table(row, n):
    global DHT_list, numOfStormEvents

    s = next_prime(2 * numOfStormEvents)
    event_id = int(row[0])
    pos = event_id % s
    id = pos % int(n)

    #print(id)

    # Get the peer with the computed identifier
    peer_to_store = next(peer for peer in DHT_list if peer.identifier == id)

    if peer_to_store.identifier == id:
        peer_to_store.local_hash_table.append(row)
    else:
        # Forward the record to the appropriate peer in the ring
        send_row_to_peer(peer_to_store, row)

def send_row_to_peer(peer, row):
    """Forward the record to the appropriate peer in the ring."""
    current_peer = peer
    next_peer = current_peer.neighbor

    while next_peer.identifier != peer.identifier:
        # Forward the record to the next peer in the ring
        next_peer.local_hash_table.append(row)
        
        # Move to the next peer in the ring
        current_peer = next_peer
        next_peer = current_peer.neighbor

def next_prime(n):
    """Return the next prime number greater than or equal to n."""
    def is_prime(num):
        """Check if num is prime."""
        if num < 2:
            return False
        for i in range(2, int(num ** 0.5) + 1):
            if num % i == 0:
                return False
        return True
    
    while True:
        if is_prime(n):
            return n
        n += 1

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
    elif command[0] == "dht_complete":
        command_response = dht_complete(command[1])
    elif command[0] == "print_manager_status":
        command_response = print_manager_status()
    elif command[0] == "print_DHT_list":
        command_response = print_DHT_list()
    elif command[0] == "Hello":
        command_response = "Hello I am the Manager Server!\n"
    else:
        command_response = "FAILURE! Couldn't find command: {}".format(command[0])
     
    return command_response

# Print Peer List Function
def print_peer_list():
    print("---------Peer List-----------")
    for obj in peer_list:
        print("Peer name: ", obj.peername, "\nIPv4 Address: ", obj.ipv4addr, "\nManager Port: ", obj.mport, 
              "\nPeer Port: ", obj.pport, "\nStatus: ", obj.status, "\n----------------------------")
        
    return "SUCCESS! List was printed"

# Print DHT List Function
def print_DHT_list():
    print("---------DHT List-----------")
    for obj in DHT_list:
        neighbor_details = "None" if obj.neighbor is None else f"{obj.neighbor.peername}"
        
        print("Peer name: ", obj.peername, "\nIPv4 Address: ", obj.ipv4addr, "\nPeer Port: ", obj.pport, "\nIdentifier: ", obj.identifier, "\nNeighbor: ", neighbor_details , "\n----------------------------")
        
    return "SUCCESS! List was printed"


# First argument for server port
server_port = int(sys.argv[1])
server_ip = socket.gethostbyname(socket.gethostname())
print("Server: Port server is listening to: ", server_port)
print("Server address: ", server_ip)

# Create socket
server_sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

# Bind socket to localhost with port argument
server_sock.bind(('', server_port))

# Infinite loop for receiving/sending messages 
while True:
    data, client_address = server_sock.recvfrom(1024)
    clinet_message = data.decode("utf-8")  # Decode the received data
    response = command_execution(clinet_message)  # Execute the command
    server_sock.sendto(response.encode("utf-8"), client_address)
