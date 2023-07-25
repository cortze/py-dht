import time
from dht.key_store import KeyValueStore
from dht.routing_table import RoutingTable
from dht.hashes import Hash

""" DHT Client """

class DHTClient():

    """ This class represents the client that participates and interacts with the simulated DHT"""

    def __repr__(self) -> str:
        return "DHT-cli-"+self.ID

    def __init__(self, ID, network, kbucketSize:int = 20, a: int = 1, b: int = 20, stuckMaxCnt = 4):
        """ client builder -> init all the internals & compose the routing table"""
        # TODO: on the options given for the DHTClient, we could consider:
        # - latency distribution (how much time to wait before giving back any reply)
        # - Kbucket size
        self.ID = ID
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
        rtNodes = self.network.bootstrap_node(self.ID, self.k, accuracy=100)
        for node in rtNodes:
            self.rt.new_discovered_peer(node)
        # Return the summary of the RoutingTable
        return self.rt.summary()

    def lookup_for_hash(self, key: Hash):
        """ search for the closest peers to any given key, starting the lookup for the closest nodes in 
        the local routing table, and contacting Alpha nodes in parallel """
        # lookup first on our routing table
        closestNodes = self.rt.get_closest_nodes_to(key)
        closestNode = min(closestNodes, key=closestNodes.get)
        prevClosestNode = 0
        procStuckCnt = 0
        attpNodes = [] # avoid contacting twice to the same peers
        lookupSummary = {
            'targetKey': key,
            'startTime': time.time(),
            'successfulCons': 0,
            'failedCons': 0,
        }
        # Ask for the key to the closest nodes in our routing table
        # untill the no closest node was found in 3 consecutive iterations 
        while (closestNode != prevClosestNode) or (procStuckCnt < self.lookupStuckMaxCnt):
            # concurrencly of Alpha 
            concurrency = 0
            for cn in closestNodes: # close node should be sorted
                # check if we already contacted this node in the same lookup
                if cn in attpNodes:
                    continue
                try: 
                    connection = self.network.connect_to_node(self.ID, cn)
                    lookupSummary['successfulCons'] += 1 
                except ConnectionError:
                    lookupSummary['failedCons'] += 1 
                    continue
                response = connection.get_closest_nodes_to(key)
                # add the nodes to the list, sort it and update the closest node
                closestNodes.update(response)
                closestNodes = dict(sorted(closestNodes.items(), key=lambda item: item[1]))
                newCloseNode = min(closestNodes, key= closestNodes.get)
                if newCloseNode != prevClosestNode:
                    procStuckCnt = 0
                    prevClosestNode = newCloseNode
                else:
                    procStuckCnt += 1
                attpNodes.append(cn)
                concurrency += 1
                if concurrency > self.alpha:
                    break
            else:
                # limit of concurrency reached
                pass
        # finish with the summary
        lookupSummary.update({
            'finishTime': time.time(),
            'contactedNodes': len(attpNodes),
            'totalNodes': len(closestNodes),
        })
        # limit the ouput to beta number of nodes
        closestNodes.pop(list(closestNodes)[self.beta:])
        return closestNodes, lookupSummary

    def get_closest_nodes_to(self, key: Hash):
        """ return the closest nodes to a given key from the local routing table (local perception of the network) """
        # check if we actually have the value of KeyValueStore, and return the content
        closerNodes = self.rt.get_closest_nodes_to(key)
        val, _ = self.ks.read(key)
        return closerNodes, val

    def provide_block_segment(self, segment) -> dict:
        """ looks for the closest nodes in the network, and sends them a """
        provideSummary = {
            'succesNodeIDs': [],
            'failedNodeIDs': [],
            'startTime': time.time(),
        }
        segH = Hash(segment)
        closestNodes, lookupSummary = self.lookup_for_hash(segH)
        for cn in closestNodes:
            try:
                connection = self.network.connect_to_node(self.ID, cn)
                connection.store_segment(segment)
                provideSummary['succesNodeIDs'].append(cn)
            except ConnectionError:
                provideSummary['failedNodeIDs'].append(cn)

        provideSummary.update({
            'finishTime': time.time(),
            'contactedPeers': lookupSummary['contactedNodes'],
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

    def get_close_nodes_to(self, key: Hash):
        return self.to.get_closest_nodes_to(key)

    def store_segment(self, segment):
        self.to.store_segment(segment)

    def retrieve_segment(self, key: Hash):
        return self.to.retrieve_segment(key)

class DHTNetwork():
    """ serves a the shared point between all the nodes participating in the simulation,
    allows node to communicat with eachother without needing to implement an API or similar"""

    def __init__(self, networkID: int, errorRate: int):
        """ class initializer, it allows to define the networkID and the delays between nodes """
        self.networkID = networkID
        self.errorRate = errorRate
        self.nodeStore = NodeStore()
        self.errorTracker = [] # every time that an error is tracked, add it to the queue
        self.connectionTracker = [] # every time that a connection was stablished
        self.connectionCnt = 0
    
    def add_new_node(self, newNode: DHTClient):
        """ add a new node to the DHT network """
        self.nodeStore.add_node(newNode)

    def connect_to_node(self, originNode: int, targetNode: int) -> Connection:
        """ get the given DHT client from the PeerStore or raise an error """
        # increase always the total connection counter
        self.connectionCnt += 1
        # TODO: apply `errorRate` chances of not connecting a node 
        try:
            node = self.nodeStore.get_node(targetNode)
            self.connectionTracker.append({
                'time': time.time(),
                'from': originNode,
                'to': targetNode,}) 
            return Connection(self.connectionCnt, originNode, node)

        # TODO: at the moment, I only have a peer-missing error, update it to a connection error-rate (usual in Libp2p)
        except NodeNotInStoreError as e:
            connError = ConnectionError(e.missingNode, e.description, e.time)
            self.errorTracker.append({
                'time': connError.errorTime, 
                'error': connError.description})
            raise connError

    def bootstrap_node(self, nodeID: int, bucketSize:int, accuracy: int = 100):
        """ checks among all the existing nodes in the network, which are the correct ones to 
        fill up the routing table of the given node """
        # best way to know which nodes are the best nodes for a routing table, is to compose a rt itself 
        # Accuracy = How many closest peers / K closest peers do we know (https://github.com/plprobelab/network-measurements/blob/master/results/rfm19-dht-routing-table-health.md) 
        # TODO: generate a logic that selects the routing table with the given accuracy
        rt = RoutingTable(nodeID, bucketSize) 
        for node in self.nodeStore.get_nodes():
            rt.new_discovered_peer(node)
        return rt.get_routing_nodes()

    def summary(self):
        """ print the summary of what happened in the network """  
        return {
            'total_nodes': self.nodeStore.len(),
            'attempts': self.connectionCnt,
            'successful': len(self.connectionTracker),
            'failures': len(self.errorTracker)}





