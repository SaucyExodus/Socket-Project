# Manager File
import socket
import random

class Peer:
    def __init__(self, peername, ipv4addr, mport, pport, status):
        self.peername = peername
        self.ipv4addr = ipv4addr
        self.mp = mport
        self.pp = pport
        self.status = status

class DHT:
    def __init__(self, peername, n, year):
        self.peername = peername
        self.n = n
        self.year = year

# List of peers
peerList = []

dht_set_up = False

def printPeerList():
    for peer in peerList:
        print()
        print("Peer name:", peer.peername)
        print("IPv4 Address:", peer.ipv4addr)
        print("Manager Port:", peer.mp)
        print("Peer Port:", peer.pp)
        print("Status:", peer.status)
        print("----------------------------------") 

def register(peername, IPv4addr, mport, pport):
    
    # Check if peer name is alphabetic and is less than 15
    if not peername.isalpha() or len(peername) > 15:
        return "FAILURE! Peer name must be an alphabetic string of length at most 15 characters."

    # Check if mport and pport are unique
    for peer in peerList:
        if peer.mp == mport or peer.pp == pport:
            return "FAILURE! Ports must be unique."

    # Check if peername is already registered
    if any(peer.peername == peername for peer in peerList):
        return "FAILURE! Peer name already registered."

    # Register new peer
    status = "Free"
    peer1 = Peer(peername, IPv4addr, mport, pport, status)
    peerList.append(peer1)

    return "SUCCESS! Peer registered successfully."

#register("Hi", "255.255.255.255", 1, 2)
#register("Hig", "255.255.255.255", 13, 24)
#printPeerList()

def setup_dht(peername, n, year):

    global dht_set_up 

    # Check if the peer name is registered
    if not any(peer.peername == peername for peer in peerList):
        return "FAILURE! Peer name is not registered."
    
    # Check if n is less than 3
    if n < 3:
        return "FAILURE! n must be at least three."
    
    # Check if less than n users are registered with manager
    if len(peerList) < n:
        return "FAILURE! Fewer than n users are registered with the manager."
    
    # Check if the DHT has already been set up
    if dht_set_up:
        return "FAILURE! A DHT has already been set up."
    
    #dht = DHT(peername, n, year)

    # Select n-1 free users at random
    free_peers = [peer for peer in peerList if peer.status == "Free"]
    selected_peers = random.sample(free_peers, n - 1)

    # Set leader's status to Leader
    for peer in peerList:
        if peer.peername == peername:
            peer.status = "Leader"

    # Set selected peers status to InDHT
    for peer in selected_peers:
        peer.status = "InDHT"

    # List of n peers that together will construct the DHT
    dht_peers = [(peername, peer.ipv4addr, peer.pp) for peer in [next(peer for peer in peerList if peer.peername == peername)] + selected_peers]

    # Set dht_set_up to true and print list of peers
    dht_set_up = True
    return "SUCCESS! DHT is set up.", dht_peers