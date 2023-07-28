import random
import time
from dht.key_store import KeyValueStore
from dht.routing_table import RoutingTable
from dht.hashes import Hash

""" DHT Client """


class DHTClient:

    """ This class represents the client that participates and interacts with the simulated DHT"""

    def __repr__(self) -> str:
        return "DHT-cli-"+self.ID

    def __init__(self, nodeid, network, kbucketSize:int = 20, a: int = 1, b: int = 20, stuckMaxCnt: int = 3):
        """ client builder -> init all the internals & compose the routing table"""
        # TODO: on the options given for the DHTClient, we could consider:
        # - latency distribution (how much time to wait before giving back any reply)
        # - Kbucket size
        self.ID = nodeid
        self.network = network
        self.k = kbucketSize
        self.rt = RoutingTable(self.ID, kbucketSize)
        self.ks = KeyValueStore()
        # DHT parameters
        self.alpha = a # the concurrency parameter per path
        self.beta = b # the number of peers closest to a target that must have responded for a query path to terminate
        self.lookupStuckMaxCnt = stuckMaxCnt # Number of maximum hops the client will do without reaching a closest peer
        # to finalize the lookup process

    def bootstrap(self) -> str:
        """ Initialize the RoutingTable from the given network and return the count of nodes per kbucket""" 
        rtNodes = self.network.bootstrap_node(self.ID, self.k)
        for node in rtNodes:
            self.rt.new_discovered_peer(node)
        # Return the summary of the RoutingTable
        return self.rt.summary()

    def lookup_for_hash(self, key: Hash):
        """ search for the closest peers to any given key, starting the lookup for the closest nodes in 
        the local routing table, and contacting Alpha nodes in parallel """
        lookupSummary = {
            'targetKey': key,
            'startTime': time.time(),
            'connectionAttempts': 0,
            'successfulCons': 0,
            'failedCons': 0,
        }

        closestNodes = self.rt.get_closest_nodes_to(key)
        nodesToTry = closestNodes.copy()
        newNodes = {}
        lookupValue = ""
        stuckCnt = 0
        concurrency = 0
        def has_closer_nodes(prev, new):
            for node, dist in new.items():
                if node in prev:
                    continue
                for _, existingDist in prev.items():
                    if dist < existingDist:
                        return True
                    else:
                        continue
            return False

        def not_tracked(total, newones):
            newNodes = {}
            for node, dist in newones.items():
                if node not in total:
                    newNodes[node] = dist
            return newNodes
        
        while (stuckCnt < self.lookupStuckMaxCnt) and (len(nodesToTry) > 0) :
            # ask queued nodes to try
            for node in list(nodesToTry):
                lookupSummary['connectionAttempts'] += 1
                try: 
                    connection = self.network.connect_to_node(self.ID, node)
                    newNodes, val, ok = connection.get_closest_nodes_to(key)
                    if ok:
                        lookupValue = val
                    lookupSummary['successfulCons'] += 1
                    if has_closer_nodes(closestNodes, newNodes):
                        stuckCnt = 0
                        nonTrackedNodes = not_tracked(closestNodes, newNodes)
                        closestNodes.update(nonTrackedNodes)
                        nodesToTry.update(nonTrackedNodes)
                        nodesToTry = dict(sorted(nodesToTry.items(), key= lambda item: item[1]))
                    else: 
                        stuckCnt += 1
                except ConnectionError:
                    lookupSummary['failedCons'] += 1
                    stuckCnt += 1
                concurrency += 1 
                if concurrency >= self.alpha:
                    break
            else:
                # concurrency limit reached, refresh who to ask later
                pass

        # finish with the summary
        lookupSummary.update({
            'finishTime': time.time(),
            'totalNodes': len(closestNodes),
            'value': lookupValue,
        })
        # limit the ouput to beta number of nodes
        closestNodes = dict(sorted(closestNodes.items(), key=lambda item: item[1])[:self.beta])
        return closestNodes, lookupValue, lookupSummary

    def get_closest_nodes_to(self, key: Hash):
        """ return the closest nodes to a given key from the local routing table (local perception of the network) """
        # check if we actually have the value of KeyValueStore, and return the content
        closerNodes = self.rt.get_closest_nodes_to(key)
        val, ok = self.ks.read(key)
        return closerNodes, val, ok

    def provide_block_segment(self, segment) -> dict:
        """ looks for the closest nodes in the network, and sends them a """
        provideSummary = {
            'succesNodeIDs': [],
            'failedNodeIDs': [],
            'startTime': time.time(),
        }
        segH = Hash(segment)
        closestNodes, _, lookupSummary = self.lookup_for_hash(segH)
        for cn in closestNodes:
            try:
                connection = self.network.connect_to_node(self.ID, cn)
                connection.store_segment(segment)
                provideSummary['succesNodeIDs'].append(cn)
            except ConnectionError:
                provideSummary['failedNodeIDs'].append(cn)

        provideSummary.update({
            'closestNodes': closestNodes.keys(),
            'finishTime': time.time(),
            'contactedPeers': lookupSummary['connectionAttempts'],
        })
        return provideSummary

    def store_segment(self, segment):
        segH = Hash(segment)
        self.ks.add(key=segH, value=segment)

    def retrieve_segment(self, key: Hash):
        seg, ok = self.ks.read(key)
        return seg, ok
 
""" Node Store """ 

class NodeNotInStoreError(Exception):
    """ custom exection to handle missing nodes """
    def __init__(self, nodeID: int, errorTime):
        self.missingNode = nodeID
        self.time = errorTime

    def description(self) -> str:
        return "node not in node-store"

class NodeStore():
    """ Storage unit of all the available nodes in the network, or that a client saw """
    
    def __init__(self):
        """ init the memory NodeStore """
        self.nodes = {}

    def add_node(self, node: DHTClient):
        """ add or overide existing info about a node """
        self.nodes[node.ID] = node

    def get_node(self, nodeID: int) -> DHTClient:
        """ retrieve a given peer from the nodeStore or raise a missing peer error """
        try:
            return self.nodes[nodeID]
        except KeyError:
            raise NodeNotInStoreError(nodeID, time.time())

    def get_nodes(self):
        return self.nodes.keys()

    def len(self):
        """ return the size of the network """ 
        return len(self.nodes)


""" DHTNetwork """ 

class ConnectionError(Exception):
    """ custom connection error exection to notify an errored connection """ 
    def __init__(self, nodeID: int, error, time):
        self.erroredNode = nodeID
        self.error = error
        self.errorTime = time
    
    def description(self) -> str:
        return f"unable to connect node {self.erroredNode}. {self.error}"


class Connection():
    """ connection simbolizes the interaction that 2 DHTClients could have with eachother """
    def __init__(self, connID: int, f: int, to: DHTClient):
        self.connID = connID
        self.f = f
        self.to = to

    def get_closest_nodes_to(self, key: Hash):
        return self.to.get_closest_nodes_to(key)

    def store_segment(self, segment):
        self.to.store_segment(segment)

    def retrieve_segment(self, key: Hash):
        return self.to.retrieve_segment(key)


class DHTNetwork:
    """ serves a the shared point between all the nodes participating in the simulation,
    allows node to communicat with eachother without needing to implement an API or similar"""

    def __init__(self, networkID: int, errorRate: int):
        """ class initializer, it allows to define the networkID and the delays between nodes """
        self.networkID = networkID
        self.errorRate = errorRate / 100.0 # get the error rate in a float [0, 1] to be comparable with random.random
        self.nodeStore = NodeStore()
        self.errorTracker = [] # every time that an error is tracked, add it to the queue
        self.connectionTracker = [] # every time that a connection was stablished
        self.connectionCnt = 0
    
    def add_new_node(self, newNode: DHTClient):
        """ add a new node to the DHT network """
        self.nodeStore.add_node(newNode)

    def connect_to_node(self, originNode: int, targetNode: int):
        """ get the given DHT client from the PeerStore or raise an error """
        # increase always the total connection counter
        self.connectionCnt += 1
        try:
            # check the error rate (avoid stablishing the connection if there is an error)
            errorGuess = random.random()
            if errorGuess < self.errorRate:
                raise ConnectionError(targetNode, "simulated error", time.time())
            node = self.nodeStore.get_node(targetNode)
            self.connectionTracker.append({
                'time': time.time(),
                'from': originNode,
                'to': targetNode,})
            return Connection(self.connectionCnt, originNode, node)

        # TODO: at the moment, I only have a peer-missing error, update it to a connection error-rate (usual in Libp2p)
        except NodeNotInStoreError as e:
            connerror = ConnectionError(e.missingNode, e.description, e.time)
            self.errorTracker.append({
                'time': connerror.errorTime,
                'error': connerror.description})
            raise connerror

    def bootstrap_node(self, nodeid: int, bucketsize: int): # ( accuracy: int = 100 )
        """ checks among all the existing nodes in the network, which are the correct ones to 
        fill up the routing table of the given node """
        # best way to know which nodes are the best nodes for a routing table, is to compose a rt itself 
        # Accuracy = How many closest peers / K closest peers do we know (https://github.com/plprobelab/network-measurements/blob/master/results/rfm19-dht-routing-table-health.md) 
        # TODO: generate a logic that selects the routing table with the given accuracy
        rt = RoutingTable(nodeid, bucketsize)
        for node in self.nodeStore.get_nodes():
            rt.new_discovered_peer(node)
        return rt.get_routing_nodes()

    def summary(self):
        """ return the summary of what happened in the network """  
        return {
            'total_nodes': self.nodeStore.len(),
            'attempts': self.connectionCnt,
            'successful': len(self.connectionTracker),
            'failures': len(self.errorTracker)}

    def len(self) -> int:
        return self.nodeStore.len()



