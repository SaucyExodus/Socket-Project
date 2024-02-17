# Peer File
import socket
import random

class Peer:
    def __init__(self, peername, ipv4addr, mport, pport, status):
        self.peername = peername
        self.ipv4addr = ipv4addr
        self.mp = mport
        self.pp = pport
        self.status = status
        self.id = None
        self.rightNeighbor = None


