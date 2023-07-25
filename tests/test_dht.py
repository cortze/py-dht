import random
from types import ClassMethodDescriptorType
import unittest
from dht.routing_table import RoutingTable
from dht.dht import DHTClient, ConnectionError, DHTNetwork
from dht.hashes import Hash

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
        network, _ = generateNetwork(k, size, id, errorRate)
        
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
        """ test that the routing tables for each nodeID are correctly initialized """
        k = 2
        size = 20
        errorRate = 80
        network, nodes = generateNetwork(k, size, 0, errorRate)

        for node in nodes:
            summary = node.bootstrap()
            rt_aux = RoutingTable(node.ID, k)
            for otherNode in nodes:
                rt_aux.new_discovered_peer(otherNode.ID)
            # compare the rt from the network with the one from the real RoutingTable
            self.assertEqual(summary, rt_aux.summary())

    def test_dht_interop(self): 
        """ test if the nodes in the network actually route to the closest peer, and implicidly, if the DHTclient interface works """ 
        k = 10
        size = 500 
        id = 0
        errorRate = 80
        _, nodes = generateNetwork(k, size, id, errorRate)
        for node in nodes:
            node.bootstrap()
   
        randomSegment = "this is a simple segment of code"
        segH = Hash(randomSegment)
        # use random node as lookup point
        randomNodeID = random.sample(range(1, size), 1)[0]
        rNode = nodes[randomNodeID]
        self.assertNotEqual(rNode.network.len(), 0)
        
        closestNodes, val, summary = rNode.lookup_for_hash(key=segH)
        self.assertEqual(val, "") # empty val, nothing stored yet
        self.assertEqual(len(closestNodes), k)
        print(f"lookup operation with {size} nodes done in {summary['finishTime'] - summary['startTime']}")

        # validation of the lookup closestNodes vs the actual closestNodes in the network
        validationClosestNodes = {}
        for node in nodes:
            nodeH = Hash(node.ID)
            dist = nodeH.xor_to_hash(segH)
            validationClosestNodes[node.ID] = dist
        
        validationClosestNodes = dict(sorted(validationClosestNodes.items(), key=lambda item: item[1])[:k])
        for i, node in enumerate(closestNodes):
            self.assertEqual((node in validationClosestNodes), True) 

 
    def test_dht_provide_and_lookup(self): 
        """ test if the nodes in the network actually route to the closest peer, and implicidly, if the DHTclient interface works """ 
        k = 10
        size = 500 
        id = 0
        errorRate = 80
        _, nodes = generateNetwork(k, size, id, errorRate)
        for node in nodes:
            node.bootstrap()
   
        randomSegment = "this is a simple segment of code"
        segH = Hash(randomSegment)
        # use random node as lookup point
        publisherNodeID = random.sample(range(1, size), 1)[0]
        pNode = nodes[publisherNodeID]
        self.assertNotEqual(pNode.network.len(), 0)
        
        provideSummary = pNode.provide_block_segment(randomSegment)
        self.assertEqual(len(provideSummary["closestNodes"]), k)
        print(f"provide operation with {size} nodes done in {provideSummary['finishTime'] - provideSummary['startTime']}")
        
        interestedNodeID = random.sample(range(1, size), 1)[0]
        iNode = nodes[interestedNodeID]
        closestNodes, val, summary = iNode.lookup_for_hash(key=segH)
        self.assertEqual(randomSegment, val)

def generateNetwork(k, size, id, errorRate):
    network = DHTNetwork(id, errorRate)
    nodeIDs = range(1, size+1, 1)
    nodes = []
    for i in nodeIDs:
        n = DHTClient(id=i, network=network, kbucketSize=k, a=2, b=k, stuckMaxCnt=5)
        network.add_new_node(n)
        nodes.append(n)
    return network, nodes



