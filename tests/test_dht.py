import random
import unittest
from dht.dht import DHTClient, ConnectionError, DHTNetwork

class TestNetwork(unittest.TestCase):

    def test_network(self):
        # configuration of the DHTnetwork
        """ 
        # TODO: add all the delay part
        # configuration of the delays
        delayRange = range()
        randomSeed = ""
        random.seed(randomSeed)
        """ 
        k = 20
        size = 200
        id = 0
        errorRate = 80
        network = generateNetwork(k, size, id, errorRate)
        
        # check total size of the network
        totalnodes = network.nodeStore.len()
        self.assertEqual(size, totalnodes)

        # check if we could have the correct rt for any specific nodeIDs
        for nodeID in range(k):
            # the test should actually fail if a Exception is raised
            to = random.sample(range(size-1), 1)
            _ = network.connect_to_node(nodeID, to[0])
            
        # force the failure of the connections attempting to connect a peer that doesn't exist
        with self.assertRaises(ConnectionError):
            _ = network.connect_to_node(1, size+1)

        # check the summary of the network
        summary = network.summary()
        self.assertEqual(summary['total_nodes'], size)
        self.assertEqual(summary['attempts'], k+1)
        self.assertEqual(summary['successful'], k)
        self.assertEqual(summary['failures'], 1)

    def test_network_initialization(self):
        k = 2
        size = 10
        errorRate = 80
        network = generateNetwork(k, size, 0, errorRate)
         


    def test_dht_client(self): 
        k = 20
        size = 200
        id = 0
        errorRate = 80
        network = generateNetwork(k, size, id, errorRate)
        
    

def generateNetwork(k, size, id, errorRate) -> DHTNetwork:
    network = DHTNetwork(id, errorRate)
    nodeIDs = range(1, size+1, 1)
    for i in nodeIDs:
        n = DHTClient(i, k, 0)
        network.add_new_node(n)

    return network

