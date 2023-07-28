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
            'aggrDelay': 0,
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
                    connection, conndelay = self.network.connect_to_node(self.ID, node)
                    lookupSummary['aggrDelay'] += conndelay
                    # TODO: should I reuse the same delay from stablishing the connection for the rest of the operations?
                    newNodes, val, ok, closestDelay = connection.get_closest_nodes_to(key)
                    if ok:
                        lookupValue = val
                    lookupSummary['aggrDelay'] += closestDelay
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
        # limit the output to beta number of nodes
        closestNodes = dict(sorted(closestNodes.items(), key=lambda item: item[1])[:self.beta])
        # the aggregated delay of the operation is included with the summary `lookupSummary['aggrDelay']`
        return closestNodes, lookupValue, lookupSummary, lookupSummary['aggrDelay']

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
            'aggrDelay': 0,
        }
        segH = Hash(segment)
        closestNodes, _, lookupSummary, lookupDelay = self.lookup_for_hash(segH)
        provideSummary['aggrDelay'] += lookupDelay
        for cn in closestNodes:
            try:
                connection, conneDelay = self.network.connect_to_node(self.ID, cn)
                provideSummary['aggrDelay'] += conneDelay
                storeDelay = connection.store_segment(segment)
                provideSummary['aggrDelay'] += storeDelay
                provideSummary['succesNodeIDs'].append(cn)
            except ConnectionError:
                provideSummary['failedNodeIDs'].append(cn)

        provideSummary.update({
            'closestNodes': closestNodes.keys(),
            'finishTime': time.time(),
            'contactedPeers': lookupSummary['connectionAttempts'],
        })
        return provideSummary, provideSummary['aggrDelay']

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
    def __init__(self, connID: int, f: int, to: DHTClient, delayRange):
        self.connID = connID
        self.f = f
        self.to = to
        if delayRange is not None:
            self.delay = random.sample(delayRange, 1)[0]
        else:
            self.delay = 0  # ms

    def get_closest_nodes_to(self, key: Hash):
        closerNodes, val, ok = self.to.get_closest_nodes_to(key)
        return closerNodes, val, ok, self.delay

    def store_segment(self, segment):
        self.to.store_segment(segment)
        return self.delay

    def retrieve_segment(self, key: Hash):
        seg, ok = self.to.retrieve_segment(key)
        return seg, ok, self.delay


class DHTNetwork:
    """ serves a the shared point between all the nodes participating in the simulation,
    allows node to communicat with eachother without needing to implement an API or similar"""

    def __init__(self, networkID: int, errorRate: int, delayRage):
        """ class initializer, it allows to define the networkID and the delays between nodes """
        self.networkID = networkID
        self.errorRate = errorRate  # %
        self.delayRange = delayRage  # list() in ms -> i.e., (5, 100) ms | None
        self.nodeStore = NodeStore()
        self.errorTracker = [] # every time that an error is tracked, add it to the queue
        self.connectionTracker = [] # every time that a connection was stablished
        self.connectionCnt = 0
    
    def add_new_node(self, newNode: DHTClient):
        """ add a new node to the DHT network """
        self.nodeStore.add_node(newNode)

    def connect_to_node(self, originNode: int, targetNode: int):
        """ get connection to the DHTclient target from the PeerStore
         and an associated delay or raise an error """
        self.connectionCnt += 1
        try:
            # check the error rate (avoid stablishing the connection if there is an error)
            if random.randint(0, 99) < self.errorRate:
                connerror = ConnectionError(targetNode, "simulated error", time.time())
                self.errorTracker.append(connerror)
                raise connerror
            node = self.nodeStore.get_node(targetNode)
            connection = Connection(self.connectionCnt, originNode, node, self.delayRange)
            self.connectionTracker.append({
                'time': time.time(),
                'from': originNode,
                'to': targetNode,
                'delay': connection.delay})
            return connection, connection.delay

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



