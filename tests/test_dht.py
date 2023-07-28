import random
from types import ClassMethodDescriptorType
import unittest
from dht.routing_table import RoutingTable
from dht.dht import DHTClient, ConnectionError, DHTNetwork
from dht.hashes import Hash

class TestNetwork(unittest.TestCase):

    def test_network(self):
        # configuration of the DHTnetwork
        k = 20
        size = 200
        id = 0
        errorRate = 0  # apply an error rate of 0 (to check if the logic pases)
        delayRange = None  # ms
        network, _ = generateNetwork(k, size, id, errorRate, delayRange)
        
        # check total size of the network
        totalnodes = network.nodeStore.len()
        self.assertEqual(size, totalnodes)

        # check if we could have the correct rt for any specific nodeIDs
        for nodeID in range(k):
            # the test should actually fail if a Exception is raised
            to = random.sample(range(size-1), 1)
            _, _ = network.connect_to_node(nodeID, to[0])
            
        # force the failure of the connections attempting to connect a peer that doesn't exist
        with self.assertRaises(ConnectionError):
            _, _ = network.connect_to_node(1, size+1)

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
        errorRate = 0 # apply an error rate of 0 (to check if the logic pases)
        delayRange = None  # ms
        network, nodes = generateNetwork(k, size, 0, errorRate, delayRange)

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
        errorRate = 0 # apply an error rate of 0 (to check if the logic pases)
        delayRange = None  # ms
        _, nodes = generateNetwork(k, size, id, errorRate, delayRange)
        for node in nodes:
            node.bootstrap()
   
        randomSegment = "this is a simple segment of code"
        segH = Hash(randomSegment)
        # use random node as lookup point
        randomNodeID = random.sample(range(1, size), 1)[0]
        rNode = nodes[randomNodeID]
        self.assertNotEqual(rNode.network.len(), 0)
        
        closestNodes, val, summary, _ = rNode.lookup_for_hash(key=segH)
        self.assertEqual(val, "") # empty val, nothing stored yet
        self.assertEqual(len(closestNodes), k)
        # print(f"lookup operation with {size} nodes done in {summary['finishTime'] - summary['startTime']}")

        # validation of the lookup closestNodes vs the actual closestNodes in the network
        validationClosestNodes = {}
        for node in nodes:
            nodeH = Hash(node.ID)
            dist = nodeH.xor_to_hash(segH)
            validationClosestNodes[node.ID] = dist
        
        validationClosestNodes = dict(sorted(validationClosestNodes.items(), key=lambda item: item[1])[:k])
        for i, node in enumerate(closestNodes):
            self.assertEqual((node in validationClosestNodes), True)

    def test_dht_error_rate_on_connection(self):
        """ test if the nodes in the network actually route to the closest peer, and implicidly, if the DHTclient interface works """
        k = 1
        size = 2
        id = 0
        errorrate = 50   # apply an error rate of 0 (to check if the logic pases)
        delayRange = None  # ms
        network, nodes = generateNetwork(k, size, id, errorrate, delayRange)
        for node in nodes:
            node.bootstrap()

        successcnt = 0
        failedcnt = 0
        iterations = 1000  # for statistical robustness
        variance = 5  # %
        for i in range(iterations):
            try:
                _, _ = network.connect_to_node(nodes[0].ID, nodes[1].ID)
                successcnt += 1
            except ConnectionError as e:
                failedcnt += 1

        expected = iterations / (100/errorrate)
        allowedvar = iterations / (100/variance)
        self.assertGreater(failedcnt, expected - allowedvar)
        self.assertLess(failedcnt, expected + allowedvar)

    def test_dht_provide_and_lookup(self): 
        """ test if the nodes in the network actually route to the closest peer, and implicidly, if the DHTclient interface works """ 
        k = 10
        size = 500 
        id = 0
        errorRate = 0 # apply an error rate of 0 (to check if the logic pases)
        delayRange = None  # ms
        _, nodes = generateNetwork(k, size, id, errorRate, delayRange)
        for node in nodes:
            node.bootstrap()
   
        randomSegment = "this is a simple segment of code"
        segH = Hash(randomSegment)
        # use random node as lookup point
        publisherNodeID = random.sample(range(1, size), 1)[0]
        pNode = nodes[publisherNodeID]
        self.assertNotEqual(pNode.network.len(), 0)
        
        provideSummary, _ = pNode.provide_block_segment(randomSegment)
        self.assertEqual(len(provideSummary["closestNodes"]), k)
        # print(f"provide operation with {size} nodes done in {provideSummary['finishTime'] - provideSummary['startTime']}")
        
        interestedNodeID = random.sample(range(1, size), 1)[0]
        iNode = nodes[interestedNodeID]
        closestNodes, val, summary, _ = iNode.lookup_for_hash(key=segH)
        self.assertEqual(randomSegment, val)

    def test_aggregated_delays(self):
        """ test if the interaction between the nodes in the network actually generate a compounded delay """
        k = 10
        size = 500
        id = 0
        errorRate = 0  # apply an error rate of 0 (to check if the logic pases)
        maxDelay = 101
        minDelay = 10
        delayRange = range(minDelay, maxDelay, 10)  # ms
        _, nodes = generateNetwork(k, size, id, errorRate, delayRange)
        for node in nodes:
            node.bootstrap()

        randomSegment = "this is a simple segment of code"
        segH = Hash(randomSegment)
        # use random node as lookup point
        publisherNodeID = random.sample(range(1, size), 1)[0]
        pNode = nodes[publisherNodeID]
        self.assertNotEqual(pNode.network.len(), 0)

        provideSummary, aggrdelay = pNode.provide_block_segment(randomSegment)
        self.assertEqual(len(provideSummary["closestNodes"]), k)

        lookupPeers = provideSummary['contactedPeers']
        providePeers = len(provideSummary['succesNodeIDs'])
        totdelays = lookupPeers * 2 + providePeers + 2
        bestDelay = totdelays * minDelay
        worstDelay = totdelays * maxDelay
        self.assertGreater(aggrdelay, bestDelay)
        self.assertLess(aggrdelay, worstDelay)

        interestedNodeID = random.sample(range(1, size), 1)[0]
        iNode = nodes[interestedNodeID]
        closestNodes, val, summary, aggrdelay = iNode.lookup_for_hash(key=segH)
        self.assertEqual(randomSegment, val)

        lookupPeers = summary['successfulCons']
        totdelays = lookupPeers * 2
        bestDelay = totdelays * minDelay
        worstDelay = totdelays * maxDelay
        self.assertGreater(aggrdelay, bestDelay)
        self.assertLess(aggrdelay, worstDelay)

def generateNetwork(k, size, id, errorRate, delayRate):
    network = DHTNetwork(id, errorRate, delayRate)
    nodeIDs = range(1, size+1, 1)
    nodes = []
    for i in nodeIDs:
        n = DHTClient(nodeid=i, network=network, kbucketSize=k, a=1, b=k, stuckMaxCnt=3)
        network.add_new_node(n)
        nodes.append(n)
    return network, nodes



