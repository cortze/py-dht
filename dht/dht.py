from dht.key_store import KeyValueStore
from dht.routing_table import RoutingTable
from dht.hashes import Hash

class DHTClient():

    """ This class represents the client that participates and interacts with the simulated DHT"""

    def __repr__(self) -> str:
        return "DHT-cli-"+self.ID

    def __init__(self, ID, kbucketSize:int = 20, delay: int = 30):
        """ client builder -> init all the internals & compose the routing table"""
        
        # TODO: on the options given for the DHTClient, we could consider:
        # - latency distribution (how much time to wait before giving back any reply)
        # - Kbucket size

        self.ID = ID
        self.ks = KeyValueStore()
        self.rt = RoutingTable(self.ID, kbucketSize)
        self.delay = delay

    def (self, bootstrapNodes):
        """ Initialize the RoutingTable from the given bootstrap nodes and return the count of nodes per kbucket""" 
   
        # Return the summary of the RoutingTable
        return 
    
