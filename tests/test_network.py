import random
import unittest
import time
from collections import deque
from concurrent.futures import ProcessPoolExecutor
from dht.routing_table import RoutingTable, optimalRTforDHTcli
from dht.dht import DHTClient, ConnectionError, DHTNetwork
from dht.hashes import Hash

class TestNetwork(unittest.TestCase):

    def test_network(self):
        # configuration of the DHTnetwork
        k = 20
        size = 200
        id = 0
        errorrate = 0  # apply an error rate of 0 (to check if the logic pases)
        delayrange = None  # ms
        network, _ = generate_network(k, size, id, errorrate, delayrange)
        
        # check total size of the network
        totalnodes = network.nodestore.len()
        self.assertEqual(size, totalnodes)

        # check if we could have the correct rt for any specific nodeIDs
        for nodeID in range(k):
            # the test should actually fail if a Exception is raised
            to = random.sample(range(size), 1)
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

    def test_optimal_rt_for_dhtcli(self):
        """ test the routing table of a dht cli using the fast approach """
        k = 5
        a = 1
        b = k
        nodeid = 1
        steps4stop = 3
        size = 100

        network = DHTNetwork(0, 0, None)
        classicnode = DHTClient(nodeid, network, k, a, b, steps4stop)
        fastnode = DHTClient(nodeid, network, k, a, b, steps4stop)

        nodes = deque()
        for n in range(size):
            nodes.append((n, Hash(n)))
            classicnode.rt.new_discovered_peer(n)

        fastnode = optimalRTforDHTcli(fastnode, nodes, k)
        for n in sorted(classicnode.rt.get_routing_nodes()):
            self.assertTrue(n in fastnode.rt.get_routing_nodes())

    def test_fast_network_initialization(self):
        """ test that the routing tables for each nodeID are correctly initialized """
        k = 10
        a = 1
        b = k
        step4stop = 3
        size = 1000
        errorrate = 0  # apply an error rate of 0 (to check if the logic pases)
        delayrange = None  # ms

        network = DHTNetwork(0, errorrate, delayrange)
        network.init_with_random_peers(1, size, k, a, b, step4stop)

        for nodeid in range(size):
            node = DHTClient(nodeid, network, k, a, b, step4stop)
            _ = node.bootstrap()
            rtnodes = node.rt.get_routing_nodes()
            fastrtnodes = network.nodestore.nodes[nodeid].rt.get_routing_nodes()
            self.assertFalse(nodeid in rtnodes)
            self.assertFalse(nodeid in fastrtnodes)
            self.assertEqual(len(rtnodes), len(fastrtnodes))
            for n in rtnodes:
                self.assertTrue(n in fastrtnodes)

    def test_threaded_fast_network_initialization(self):
        """ test that the routing tables for each nodeID are correctly initialized """
        k = 10
        a = 1
        b = k
        step4stop = 3
        size = 1000
        errorrate = 0  # apply an error rate of 0 (to check if the logic pases)
        delayrange = None  # ms
        threads = 2

        network = DHTNetwork(0, errorrate, delayrange)
        network.init_with_random_peers(threads, size, k, a, b, step4stop)

        for nodeid in range(size):
            node = DHTClient(nodeid, network, k, a, b, step4stop)
            _ = node.bootstrap()
            rtnodes = node.rt.get_routing_nodes()
            fastrtnodes = network.nodestore.nodes[nodeid].rt.get_routing_nodes()
            self.assertFalse(nodeid in rtnodes)
            self.assertFalse(nodeid in fastrtnodes)
            self.assertEqual(len(rtnodes), len(fastrtnodes))
            for n in rtnodes:
                self.assertTrue(n in fastrtnodes)

    def test_threading(self):
        """ test that the routing tables for each nodeID are correctly initialized """
        k = 10
        a = 1
        b = k
        step4stop = 3
        size = 15000
        errorrate = 0  # apply an error rate of 0 (to check if the logic pases)
        delayrange = None  # ms
        threads = 12

        network = DHTNetwork(0, errorrate, delayrange)
        start = time.time()
        _ = network.init_with_random_peers(threads, size, k, a, b, step4stop)
        print(f'{size} nodes in {time.time() - start} - {threads} cores')

    def test_network_initialization(self):
        """ test that the routing tables for each nodeID are correctly initialized """
        k = 2
        size = 20
        errorrate = 0 # apply an error rate of 0 (to check if the logic pases)
        delayrange = None  # ms
        network, nodes = generate_network(k, size, 0, errorrate, delayrange)

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
        errorrate = 0 # apply an error rate of 0 (to check if the logic pases)
        delayrange = None  # ms
        _, nodes = generate_network(k, size, id, errorrate, delayrange)
        for node in nodes:
            node.bootstrap()
   
        randomsegment = "this is a simple segment of code"
        segH = Hash(randomsegment)
        # use random node as lookup point
        randomid = random.sample(range(1, size), 1)[0]
        rnode = nodes[randomid]
        self.assertNotEqual(rnode.network.len(), 0)
        
        closestnodes, val, summary, _ = rnode.lookup_for_hash(key=segH)
        self.assertEqual(val, "")  # empty val, nothing stored yet
        self.assertEqual(len(closestnodes), k)
        # print(f"lookup operation with {size} nodes done in {summary['finishTime'] - summary['startTime']}")

        # validation of the lookup closestnodes vs the actual closestnodes in the network
        validationclosestnodes = {}
        for node in nodes:
            nodeH = Hash(node.ID)
            dist = nodeH.xor_to_hash(segH)
            validationclosestnodes[node.ID] = dist
        
        validationclosestnodes = dict(sorted(validationclosestnodes.items(), key=lambda item: item[1])[:k])
        for i, node in enumerate(closestnodes):
            self.assertEqual((node in validationclosestnodes), True)

    def test_dht_error_rate_on_connection(self):
        """ test if the nodes in the network actually route to the closest peer, and implicidly, if the DHTclient interface works """
        k = 1
        size = 2
        id = 0
        errorrate = 50   # apply an error rate of 0 (to check if the logic pases)
        delayrange = None  # ms
        network, nodes = generate_network(k, size, id, errorrate, delayrange)
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
        errorrate = 0 # apply an error rate of 0 (to check if the logic pases)
        delayrange = None  # ms
        _, nodes = generate_network(k, size, id, errorrate, delayrange)
        for node in nodes:
            node.bootstrap()
   
        rsegment = "this is a simple segment of code"
        segH = Hash(rsegment)
        # use random node as lookup point
        pnodeid = random.sample(range(1, size), 1)[0]
        pnode = nodes[pnodeid]
        self.assertNotEqual(pnode.network.len(), 0)
        
        psummary, _ = pnode.provide_block_segment(rsegment)
        self.assertEqual(len(psummary["closestNodes"]), k)
        # print(f"provide operation with {size} nodes done in {provideSummary['finishTime'] - provideSummary['startTime']}")
        
        interestednodeid = random.sample(range(1, size), 1)[0]
        inode = nodes[interestednodeid]
        closestnodes, val, summary, _ = inode.lookup_for_hash(key=segH)
        self.assertEqual(rsegment, val)

    def test_aggregated_delays(self):
        """ test if the interaction between the nodes in the network actually generate a compounded delay """
        k = 10
        size = 500
        id = 0
        errorrate = 0  # apply an error rate of 0 (to check if the logic pases)
        maxDelay = 101
        minDelay = 10
        delayrange = range(minDelay, maxDelay, 10)  # ms
        _, nodes = generate_network(k, size, id, errorrate, delayrange)
        for node in nodes:
            node.bootstrap()

        randomSegment = "this is a simple segment of code"
        segH = Hash(randomSegment)
        # use random node as lookup point
        publishernodeid = random.sample(range(1, size), 1)[0]
        pnode = nodes[publishernodeid]
        self.assertNotEqual(pnode.network.len(), 0)

        providesummary, aggrdelay = pnode.provide_block_segment(randomSegment)
        self.assertEqual(len(providesummary["closestNodes"]), k)

        lookuppeers = providesummary['contactedPeers']
        providepeers = len(providesummary['succesNodeIDs'])
        totdelays = lookuppeers * 2 + providepeers + 2
        bestdelay = totdelays * minDelay
        worstdelay = totdelays * maxDelay
        self.assertGreater(aggrdelay, bestdelay)
        self.assertLess(aggrdelay, worstdelay)

        interestednodeid = random.sample(range(1, size), 1)[0]
        inode = nodes[interestednodeid]
        closestnodes, val, summary, aggrdelay = inode.lookup_for_hash(key=segH)
        self.assertEqual(randomSegment, val)

        lookuppeers = summary['successfulCons']
        totdelays = lookuppeers * 2
        bestdelay = totdelays * minDelay
        worstdelay = totdelays * maxDelay
        self.assertGreater(aggrdelay, bestdelay)
        self.assertLess(aggrdelay, worstdelay)

def generate_network(k, size, id, errorrate, delayrate):
    network = DHTNetwork(id, errorrate, delayrate)
    nodeids = range(0, size, 1)
    nodes = []
    for i in nodeids:
        n = DHTClient(nodeid=i, network=network, kbucketsize=k, a=1, b=k, steptostop=3)
        network.add_new_node(n)
        nodes.append(n)
    return network, nodes



