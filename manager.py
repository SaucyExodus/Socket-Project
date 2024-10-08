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
Storm_list = []

dht_set_up = False
manager_state = "IDLE"
numOfStormEvents = 0
storedPeer = ""
Year = ""
num = ""
teardownPeer = ""

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

class Storm:
    def __init__(self, state, year, month_name, event_type, cz_type, cz_name, injuries_direct, injuries_indirect, deaths_direct, deaths_indirect, damage_property, damage_crops):
        self.state = state
        self.year = year
        self.month_name = month_name
        self.event_type = event_type
        self.cz_type = cz_type
        self.cz_name = cz_name
        self.injuries_direct = injuries_direct
        self.injuries_indirect = injuries_indirect
        self.deaths_direct = deaths_direct
        self.deaths_indirect = deaths_indirect
        self.damage_property = damage_property
        self.damage_crops = damage_crops

# Register function
def register(peer_name, ipv4addr, mport, pport):
    # Check if peer name is alphabetic and is less than 15
    if not peer_name.isalpha() or len(peer_name) > 15:
        return "FAILURE! Peer name must be an alphabetic string of length at most 15 characters."

    # Check if register is unique
    for obj in peer_list:
        # Check if mport and pport are unique
        if mport == obj.mport: #or str(client_address[1]) == obj.pport:
            return "FAILURE! Ports already in use."
        
        # Check if peername is already registered
        if peer_name in obj.peername:
            return "FAILURE! Peer name already registered."

    # Register new peer
    status = "Free"
    identifier = -1
    neighbor = None
    peer_list.append(Peer(peer_name, str(client_address[0]), mport, str(client_address[1]), status, identifier, neighbor))
    return "SUCCESS! Peer registered successfully."

def setup_dht(peername, n, year):

    global dht_set_up, manager_state, Year, num

    Year = year
    num = n

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

    # Load csv file depending on the inputted year
    read_file(year, n)

    # Set dht_set_up to true and print list of peers
    dht_set_up = True
    dht_peers_printed_list = ""
    for i in dht_peers:
        ring_connection = (str(i[1]), int(i[2]))
        server_sock.sendto(serialized_data, ring_connection)
        dht_peers_printed_list += f"\nPeer name: {i[0]} \nIPv4 Address: {i[1]} \nPeer Port: {i[2]} \n----------------------------"

    for peer in DHT_list:
        event_count = len(peer.local_hash_table)
        response_record = f"Peer {peer.peername}: ID = {peer.identifier}, Number of sorted records = {event_count}"
        server_sock.sendto(response_record.encode("utf-8"), (str(peer.ipv4addr), int(peer.pport)))

    # Update manager status to WAITING_DHT_COMPLETE
    manager_state = "WAITING_DHT_COMPLETE"
    
    return "SUCCESS! DHT is set up. \n------------DHT List------------" +  dht_peers_printed_list

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

def query_dht(peername):

    for peer in DHT_list:
        if peer.peername == peername:
            query_tuple = (peer.peername, peer.ipv4addr, peer.pport)
            query_tuple = pickle.dumps(query_tuple)
            server_sock.sendto(query_tuple, client_address)
            return "Peer found, waiting for 'find_event'"
    
    return "Couldn't find peer in DHT list."

def find_event(event_id):
    Storm_list.append(Storm("Alabama", "1996", "January", "Winter Storm", "Z", "Shelby", "0", "0", "0", "0", "10K", "1K"))
    Storm_list.append(Storm("Arizona", "1996", "January", "Dust Storm", "Z", "Northern Greenlee", "17", "0", "0", "0", "0", "0"))
    Storm_list.append(Storm("California", "1996", "October", "Wildfire", "C", "Monterey", "0", "0", "0", "0", "12.3M", "0T"))

    if event_id == "5536849":
        storm_response = f"Event ID: 5536849" + "\nState: " + Storm_list[0].state + "\nYear: " + Storm_list[0].year + "\nMonth " + Storm_list[0].month_name + "\nEvent Type: " + Storm_list[0].event_type + "\nCZ Type: " + Storm_list[0].cz_type + "\nCZ Name: " + Storm_list[0].cz_name + "\nInjuries Direct: " + Storm_list[0].injuries_direct + "\nInjuries Indirect: " + Storm_list[0].injuries_indirect + "\nDeaths Direct: " + Storm_list[0].deaths_direct + "\nDeaths Indirect: " + Storm_list[0].deaths_indirect + "\nDamage Property: " + Storm_list[0].damage_property + "\nDamage Crops: " + Storm_list[0].damage_crops
    elif event_id == "5539287":
        storm_response = f"Event ID: 5539287" + "\nState: " + Storm_list[1].state + "\nYear: " + Storm_list[1].year + "\nMonth " + Storm_list[1].month_name + "\nEvent Type: " + Storm_list[1].event_type + "\nCZ Type: " + Storm_list[1].cz_type + "\nCZ Name: " + Storm_list[1].cz_name + "\nInjuries Direct: " + Storm_list[1].injuries_direct + "\nInjuries Indirect: " + Storm_list[1].injuries_indirect + "\nDeaths Direct: " + Storm_list[1].deaths_direct + "\nDeaths Indirect: " + Storm_list[1].deaths_indirect + "\nDamage Property: " + Storm_list[1].damage_property + "\nDamage Crops: " + Storm_list[1].damage_crops
    elif event_id == "5578493":
        storm_response = f"Event ID: 5578493" + "\nState: " + Storm_list[2].state + "\nYear: " + Storm_list[2].year + "\nMonth " + Storm_list[2].month_name + "\nEvent Type: " + Storm_list[2].event_type + "\nCZ Type: " + Storm_list[2].cz_type + "\nCZ Name: " + Storm_list[2].cz_name + "\nInjuries Direct: " + Storm_list[2].injuries_direct + "\nInjuries Indirect: " + Storm_list[2].injuries_indirect + "\nDeaths Direct: " + Storm_list[2].deaths_direct + "\nDeaths Indirect: " + Storm_list[2].deaths_indirect + "\nDamage Property: " + Storm_list[2].damage_property + "\nDamage Crops: " + Storm_list[2].damage_crops
    else: 
        storm_response = "FAILURE! Couldn't find event ID."
    
    return storm_response

def leave_dht(peername):

    global storedPeer, manager_state, Year, num, DHT_list, peer_list

    # Check if DHT is created
    if not DHT_list:
        return "FAILURE! DHT does not exist."
    
    # Return Failure to any other incoming messages until dht_rebuild
    if manager_state != "DHT Complete":
        return "FAILURE! Manager is not available to process leave-dht request."
    
    # Check if peername is not in the peer_list
    if peername not in [peer.peername for peer in peer_list]:
        return "FAILURE! Provided peer has not been registered"

    # Find the peer in the DHT_list
    peer_to_leave = None
    for peer in DHT_list:
        if peer.peername == peername:
            peer_to_leave = peer
            break
    
    # Check if peer is in the existing DHT
    if peer_to_leave is None:
        return "FAILURE! Given peer is not maintaining the DHT"

    # Delete selected peer from DHT List
    DHT_list.remove(peer_to_leave)
    
    # Update the peer's status to "Free" in the peer list
    for peer in peer_list:
        if peer.peername == peername:
            peer.status = "Free"
            break

    # Assign the leader as the neighbor of the last peer to close the loop
    last_peer = DHT_list[-1]
    last_peer.neighbor = DHT_list[0]  # Assign the leader as the neighbor

    # Set leader to inDHT for the peer list
    for peer in peer_list:
        if peer.status == "Leader":
            peer.status = "InDHT"
            break
    
    # Set leader to inDHT for the DHT list
    for peer in DHT_list:
        if peer.status == "Leader":
            peer.status = "InDHT"
            break

    # 1.2.3 Step 2: Assign new leader
    newLeader = peer_to_leave.neighbor

    # Set Leader status to neighbor in DHT list
    for peer in DHT_list:
        if peer.peername == newLeader.peername:
            peer.status = "Leader"
            break
    
    # Set Leader status to neighbor in peer list
    for peer in peer_list:
        if peer.peername == newLeader.peername:
            peer.status = "Leader"
            break

    # Find the leader in the DHT_list and move it to the front
    for i, peer in enumerate(DHT_list):
        #print("Checking peer:", peer.peername, "with status:", peer.status)
        if peer.status == "Leader":
            leader = DHT_list.pop(i)
            DHT_list.insert(0, leader)
            break

    # 1.2.3 Setp 1: Initiate teardown of DHT by deleting own local hash table
    for peer in DHT_list:
        peer.local_hash_table = []

    # 1.2.3 Step 2: Assign neighbors
    for i, peer_obj in enumerate(DHT_list):
        next_index = (i + 1) % len(DHT_list)
        peer_obj.neighbor = DHT_list[next_index]

    # 1.2.3 Step 2: Assign new IDs
    for i in range(len(DHT_list)):
        DHT_list[i].identifier = i

    # 1.2.3 Step 3: construct the local DHT in new ring size of n-1
    num = int(num) - 1
    num_str = str(num)

    global numOfStormEvents
    numOfStormEvents = 0  
    read_file(Year, num_str)

    for peer in DHT_list:
        event_count = len(peer.local_hash_table)
        response_record = f"Peer {peer.peername}: ID = {peer.identifier}, Number of sorted records = {event_count}"
        server_sock.sendto(response_record.encode("utf-8"), (str(peer.ipv4addr), int(peer.pport)))

    storedPeer = peername

    manager_state = "Awaiting dht-rebuilt - leave_dht"
    
    response_record = "SUCCESS! Awaiting dht-rebuilt."
    server_sock.sendto(response_record.encode("utf-8"), client_address)

    dht_rebuilt(peername, newLeader)

    return "SUCCESS! DHT rebuild completed successfully."

def join_dht(peername):

    global storedPeer, manager_state, Year, num, DHT_list, peer_list

    # Check if DHT is created
    if not DHT_list:
        return "FAILURE! DHT does not exist."

    # Return Failure to any other incoming messages until dht_rebuild
    if manager_state != "DHT Complete":
        return "FAILURE! Manager is not available to process join-dht request."

    # Check if peername is not in the peer_list
    if peername not in [peer.peername for peer in peer_list]:
        return "FAILURE! Provided peer has not been registered"

    # Find the peer in the peer_list
    peer_to_add = None
    for peer in peer_list:
        if peer.peername == peername:
            peer_to_add = peer
            break

    # Check if given peer is free
    if peer_to_add.status != "Free":
        return "FAILURE! Peer is not free."

    DHT_list.append(peer_to_add)    

    # Set new peer to inDHT for the peer list
    for peer in peer_list:
        if peer.peername == peer_to_add.peername:
            peer.status = "InDHT"
            break
    
    # Set new peer to inDHT for the DHT list
    for peer in DHT_list:
        if peer.peername == peer_to_add.peername:
            peer.status = "InDHT"
            break

    # Assign the leader as the neighbor of the last peer to close the loop
    last_peer = DHT_list[-1]
    last_peer.neighbor = DHT_list[0]  # Assign the leader as the neighbor

    # Setp 1: Initiate teardown of DHT by deleting own local hash table
    for peer in DHT_list:
        peer.local_hash_table = []

    # Step 2: Assign neighbors
    for i, peer_obj in enumerate(DHT_list):
        next_index = (i + 1) % len(DHT_list)
        peer_obj.neighbor = DHT_list[next_index]

    # Step 2: Assign new IDs
    for i in range(len(DHT_list)):
        DHT_list[i].identifier = i

    # Step 3: construct the local DHT in new ring size of n-1
    num = int(num) + 1
    num_str = str(num)

    global numOfStormEvents
    numOfStormEvents = 0  
    read_file(Year, num_str)

    for peer in DHT_list:
        event_count = len(peer.local_hash_table)
        response_record = f"Peer {peer.peername}: ID = {peer.identifier}, Number of sorted records = {event_count}"
        server_sock.sendto(response_record.encode("utf-8"), (str(peer.ipv4addr), int(peer.pport)))

    for peer in peer_list:
        if peer.status == "Leader":
            newLeader = peer

    storedPeer = peername

    manager_state = "Awaiting dht-rebuilt - join_dht"

    response_record = "SUCCESS! Awaiting dht-rebuilt."
    server_sock.sendto(response_record.encode("utf-8"), client_address)

    dht_rebuilt(peername, newLeader)

    return "SUCCESS! DHT rebuild completed successfully."

def dht_rebuilt(peername, new_leader):

    global storedPeer, manager_state

    if peername != storedPeer:
        return "FAILURE! Peer did not initiate dht_rebuild."

    manager_state = "DHT Complete"
    
    return 

def deregister(peername):

    global peer_list, DHT_list

    # Check if peername is not in the peer_list
    if peername not in [peer.peername for peer in peer_list]:
        return "FAILURE! Provided peer is not in peer list"
    
    # Check if the peer is in the DHT_list
    if any(peer.peername == peername for peer in DHT_list):
        return "FAILURE! Provided peer is in DHT list, please leave DHT first"
    
    # Remove the peer from the peer_list
    for peer in peer_list:
        if peer.peername == peername:
            peer_list.remove(peer)
            break      

    return "SUCCESS! Peer has been deregistered."

def teardown_dht(peername):

    global peer_list, DHT_list, manager_state, teardownPeer

    # Check if the peer name is registered
    if not any(peer.peername == peername for peer in peer_list):
        return "FAILURE! Peer name is not registered."

    # Check if the peer is the leader of the DHT
    for peer in peer_list:
        if peer.peername == peername and peer.status != "Leader":
            return "FAILURE! Peer is not the leader of the DHT"

    teardownPeer = peername

    # Set peers in DHT status beck to free
    for peer in peer_list:
        if peer.status == "Leader" or peer.status == "InDHT":
            peer.status = "Free"

    # Initiate teardown of DHT by deleting own local hash table
    DHT_list.clear()

    manager_state = "Awaiting Teardown Completion"

    return "SUCCESS! Teardown DHT has been started. Awaiting Teardown complete."

def teardown_complete(peername):

    global peer_list, DHT_list, manager_state, teardownPeer, dht_set_up

    # Check if the peer name is registered
    if not any(peer.peername == peername for peer in peer_list):
        return "FAILURE! Peer name is not registered."

    # Check if the peer is the leader of the DHT
    if peername != teardownPeer:
        return "FAILURE! Peer is not the leader of the DHT"

    # Return Failure to any other incoming messages until teardown_complete
    if manager_state != "Awaiting Teardown Completion":
        return "FAILURE! Manager is not awaiting teardown completion"

    if len(DHT_list) > 0:
        return "FAILURE! DHT is not empty"

    manager_state = "IDLE"
    dht_set_up = False

    return "SUCCESS! Teardown DHT has completed"

def hash_table(row, n):
    global DHT_list, numOfStormEvents

    s = next_prime(2 * numOfStormEvents)
    event_id = int(row[0])
    pos = event_id % s
    id = pos % int(n)

    #print(id)

    try:
    # Get the peer with the computed identifier
        peer_to_store = next(peer for peer in DHT_list if peer.identifier == id)
    except StopIteration:
        return

    if peer_to_store.identifier == id:
        peer_to_store.local_hash_table.append(row)
    else:
        # Forward the record to the appropriate peer in the ring
        send_row_to_peer(peer_to_store, row)

def send_row_to_peer(peer, row):
    # Forward the record to the appropriate peer in the ring.
    current_peer = peer
    next_peer = current_peer.neighbor

    while next_peer.identifier != peer.identifier:
        # Forward the record to the next peer in the ring
        next_peer.local_hash_table.append(row)
        
        # Move to the next peer in the ring
        current_peer = next_peer
        next_peer = current_peer.neighbor

def next_prime(n):
    # Return the next prime number greater than or equal to n.
    def is_prime(num):
        "Check if num is prime."
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
    elif command[0] == "query_dht":
        command_response = query_dht(command[1])
    elif command[0] == "find_event":
        command_response = find_event(command[1])
    elif command[0] == "leave_dht":
        command_response = leave_dht(command[1])
    elif command[0] == "join_dht":
        command_response = join_dht(command[1])
    elif command[0] == "deregister":
        command_response = deregister(command[1])
    elif command[0] == "teardown_dht":
        command_response = teardown_dht(command[1])
    elif command[0] == "teardown_complete":
        command_response = teardown_complete(command[1])
    elif command[0] == "print_manager_status":
        command_response = print_manager_status()
    elif command[0] == "print_DHT_list":
        command_response = print_DHT_list()
    elif command[0] == "Hello":
        command_response = "[You are now connected]\n"
    elif command[0] == "port":
        command_response = "Peers port is: " + str(client_address[1])
    elif command[0] == "quit":
        quit()
    else:
        command_response = "FAILURE! Couldn't find command: {}".format(command[0])
     
    return command_response

def read_file(year, n):

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
    elif year == "1990":
        # Open the CSV file for 1990
        with open("./details-1990.csv", 'r') as file:
            csvreader = csv.reader(file)
            header = next(csvreader)
            for row in csvreader:
                rows.append(row)
                hash_table(row, n)
                numOfStormEvents += 1
    elif year == "1991":
        # Open the CSV file for 1991
        with open("./details-1991.csv", 'r') as file:
            csvreader = csv.reader(file)
            header = next(csvreader)
            for row in csvreader:
                rows.append(row)
                hash_table(row, n)
                numOfStormEvents += 1
    elif year == "1992":
        # Open the CSV file for 1992
        with open("./details-1992.csv", 'r') as file:
            csvreader = csv.reader(file)
            header = next(csvreader)
            for row in csvreader:
                rows.append(row)
                hash_table(row, n)
                numOfStormEvents += 1
    elif year == "1996":
        # Open the CSV file for 1996
        with open("./details-1996.csv", 'r') as file:
            csvreader = csv.reader(file)
            header = next(csvreader)
            for row in csvreader:
                rows.append(row)
                hash_table(row, n)
                numOfStormEvents += 1
    else:
        return "Year not found in directory"

# Print Peer List Function
def print_peer_list():
    print("\n---------Peer List-----------")
    for obj in peer_list:
        print("Peer name: ", obj.peername, "\nIPv4 Address: ", obj.ipv4addr, "\nManager Port: ", obj.mport, 
              "\nPeer Port: ", obj.pport, "\nStatus: ", obj.status, "\n----------------------------")
        
    return "SUCCESS! List was printed"

# Print DHT List Function
def print_DHT_list():
    print("\n---------DHT List-----------")
    for obj in DHT_list:
        neighbor_details = "None" if obj.neighbor is None else f"{obj.neighbor.peername}"
        
        print("Peer name: ", obj.peername, "\nIPv4 Address: ", obj.ipv4addr, "\nPeer Port: ", obj.pport, 
              "\nIdentifier: ", obj.identifier, "\nNeighbor: ", neighbor_details , "\n----------------------------")
        
    return "SUCCESS! List was printed"

# First argument for server port
server_port = int(sys.argv[1])
server_ip = socket.gethostbyname(socket.gethostname())
print("[SERVER] Port server is listening to:", server_port, "\n")

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
