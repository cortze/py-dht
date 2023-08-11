import os
import random
import time
import multiprocessing
from concurrent import futures
from concurrent.futures import ProcessPoolExecutor
from collections import deque, defaultdict, OrderedDict
from dht.key_store import KeyValueStore
from dht.routing_table import RoutingTable, optimalRTforDHTcli
from dht.hashes import Hash

""" DHT Client """


class DHTClient:
    """ This class represents the client that participates and interacts with the simulated DHT"""
    def __repr__(self) -> str:
        return "DHT-cli-"+str(self.ID)

    def __init__(self, nodeid: int, network, kbucketsize: int = 20, a: int = 1, b: int = 20, steptostop: int = 3):
        """ client builder -> init all the internals & compose the routing table"""
        self.ID = nodeid
        self.hash = Hash(nodeid)
        self.network = network
        self.k = kbucketsize
        self.rt = RoutingTable(self.ID, kbucketsize)
        self.ks = KeyValueStore()
        # DHT parameters
        self.alpha = a  # the concurrency parameter per path
        self.beta = b  # the number of peers closest to a target that must have responded for a query path to terminate
        self.lookupsteptostop = steptostop  # Number of maximum hops the client will do without reaching a closest peer
        # to finalize the lookup process

    def bootstrap(self) -> str:
        """ Initialize the RoutingTable from the given network and return the count of nodes per kbucket""" 
        rtnodes = self.network.bootstrap_node(self.ID, self.k)
        for node in rtnodes:
            self.rt.new_discovered_peer(node)
        # Return the summary of the RoutingTable
        return self.rt.summary()

    def lookup_for_hash(self, key: Hash):
        """ search for the closest peers to any given key, starting the lookup for the closest nodes in 
        the local routing table, and contacting Alpha nodes in parallel """
        lookupsummary = {
            'targetKey': key,
            'startTime': time.time(),
            'connectionAttempts': 0,
            'connectionFinished': 0,
            'successfulCons': 0,
            'failedCons': 0,
        }

        def has_closer_nodes(prev, new):
            for n, dist in new.items():
                if n in prev:
                    continue
                for _, existingDist in prev.items():
                    if dist < existingDist:
                        return True
                    else:
                        continue
            return False

        closestnodes = self.rt.get_closest_nodes_to(key)
        nodestotry = closestnodes.copy()
        triednodes = deque()
        alpha_results = deque()
        alpha_delays = deque()
        for _ in range(self.alpha):
            alpha_delays.append(0)
        lookupvalue = ""  # TODO: hardcoded to string
        stepscnt = 0

        while (stepscnt < self.lookupsteptostop) and (len(nodestotry) > 0):
            nodes = nodestotry.copy()

            for node in nodes:
                nodestotry.pop(node)  # remove item from peers to attempt
                if node in triednodes:  # make sure we don't contact the same node twice
                    continue
                triednodes.append(node)
                lookupsummary['connectionAttempts'] += 1

                try:
                    connection, conndelay = self.network.connect_to_node(self.ID, node)
                    newnodes, val, _, closestdelay = connection.get_closest_nodes_to(key)
                    # we only want to aggregate the difference between the base + conn delay - the already aggregated one
                    # this allows to simulate de delay of a proper scheduler
                    operationdelay = (conndelay + closestdelay)
                    if len(alpha_results) < self.alpha:
                        alpha_results.append((operationdelay, newnodes, val))
                        alpha_results = deque(sorted(alpha_results, key=lambda pair: pair[0]))
                    else:
                        print("huge error here")

                except ConnectionError as e:
                    errortype, errordelay = e.get_delay()
                    alpha_results.append((errordelay, {}, ""))
                    alpha_results = deque(sorted(alpha_results, key=lambda pair: pair[0]))

                # check if the concurrency array is full
                # if so aggregate the delay and the nodes to the total and empty the slot in the deque
                if len(alpha_results) >= self.alpha:
                    # 1. Append the aggragated delay of the last node connection (suc or failed) to the smaller aggregated
                    # alpha delay (mimicking a scheduler)
                    # 2. The max value on the alpha delays will determine the aggrDelay of the lookup
                    mindelayedlookup = alpha_results.popleft()
                    minaggrdelayidx = alpha_delays.index(min(alpha_delays))
                    alpha_delays[minaggrdelayidx] += mindelayedlookup[0]

                    if mindelayedlookup[2] != "":
                        lookupvalue = mindelayedlookup[2]

                    lookupsummary['connectionFinished'] += 1
                    if len(mindelayedlookup[1]) > 0:
                        lookupsummary['successfulCons'] += 1
                    else:
                        lookupsummary['failedCons'] += 1

                    if has_closer_nodes(closestnodes, mindelayedlookup[1]):
                        stepscnt = 0
                    else:
                        stepscnt += 1

                    # even if there is any closest one, update the list as more in between might have come
                    closestnodes.update(mindelayedlookup[1])
                    nodestotry.update(mindelayedlookup[1])
                    nodestotry = OrderedDict(sorted(nodestotry.items(), key=lambda item: item[1]))
                    break

                if stepscnt >= self.lookupsteptostop:
                    break

        lookupsummary.update({
            'finishTime': time.time(),
            'totalNodes': len(closestnodes),
            'aggrDelay': max(alpha_delays),
            'value': lookupvalue,
        })
        # limit the output to beta number of nodes
        closestnodes = OrderedDict(sorted(closestnodes.items(), key=lambda item: item[1])[:self.beta])
        # the aggregated delay of the operation is included with the summary `lookupsummary['aggrDelay']`
        return closestnodes, lookupvalue, lookupsummary, lookupsummary['aggrDelay']

    def get_closest_nodes_to(self, key: Hash):
        """ return the closest nodes to a given key from the local routing table (local perception of the network) """
        # check if we actually have the value of KeyValueStore, and return the content
        closernodes = self.rt.get_closest_nodes_to(key)
        val, ok = self.ks.read(key)
        return closernodes, val, ok

    def provide_block_segment(self, segment) -> dict:
        """ looks for the closest nodes in the network, and sends them a """
        providesummary = {
            'succesNodeIDs': deque(),
            'failedNodeIDs': deque(),
            'startTime': time.time(),
            'aggrDelay': 0,
        }
        segH = Hash(segment)
        closestnodes, _, lookupsummary, lookupdelay = self.lookup_for_hash(segH)
        providesummary['aggrDelay'] += lookupdelay
        for cn in closestnodes:
            try:
                connection, conndelay = self.network.connect_to_node(self.ID, cn)
                providesummary['aggrDelay'] += conndelay
                storedelay = connection.store_segment(segment)
                providesummary['aggrDelay'] += storedelay
                providesummary['succesNodeIDs'].append(cn)
            except ConnectionError:
                providesummary['failedNodeIDs'].append(cn)

        providesummary.update({
            'closestNodes': closestnodes.keys(),
            'finishTime': time.time(),
            'contactedPeers': lookupsummary['connectionAttempts'],
        })
        return providesummary, providesummary['aggrDelay']

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
        self.nodes = defaultdict()

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
    def __init__(self, nodeid: int, error, time, d):
        self.erroredNode = nodeid
        self.error = error
        self.errorTime = time
        self.delay = 0  # ms
        if (error == "fast error") and (d is not None):
            self.delay = random.sample(d, 1)[0]
        elif (error == "slow error") and (d is not None):
            self.delay = d
    
    def description(self) -> str:
        return f"unable to connect node {self.erroredNode}. {self.error}"

    def get_delay(self):
        return self.error, self.delay

class Connection():
    """ connection simbolizes the interaction that 2 DHTClients could have with eachother """
    def __init__(self, connid: int, f: int, to: DHTClient, delayrange):
        self.connid = connid
        self.f = f
        self.to = to
        if delayrange is not None:
            self.delay = random.sample(delayrange, 1)[0]
        else:
            self.delay = 0  # ms

    def get_closest_nodes_to(self, key: Hash):
        closernodes, val, ok = self.to.get_closest_nodes_to(key)
        return closernodes, val, ok, self.delay

    def store_segment(self, segment):
        self.to.store_segment(segment)
        return self.delay

    def retrieve_segment(self, key: Hash):
        seg, ok = self.to.retrieve_segment(key)
        return seg, ok, self.delay


class DHTNetwork:
    """ serves a the shared point between all the nodes participating in the simulation,
    allows node to communicat with eachother without needing to implement an API or similar"""

    def __init__(self, networkid: int, fasterrorrate: int, slowerrorrate: int, fastdelayrange, slowdelay):
        """ class initializer, it allows to define the networkID and the delays between nodes """
        self.networkid = networkid
        self.fasterrorrate = fasterrorrate  # %
        self.slowerrorrate = slowerrorrate  # %
        self.fastdelayrange = fastdelayrange  # list() in ms -> i.e., (5, 100) ms | None
        self.slowdelay = slowdelay  # timeot delay
        self.nodestore = NodeStore()
        self.errortracker = deque()  # every time that an error is tracked, add it to the queue
        self.connectiontracker = deque()  # every time that a connection was stablished
        self.connectioncnt = 0

    def parallel_clilist_initializer(self, clilist, nodes, k):
        clis = deque(maxlen=len(clilist))
        for cli in clilist:
            clis.append(optimalRTforDHTcli(cli, nodes, k))
        return clis

    def init_with_random_peers(self, processes: int, nodesize: int, bsize: int, a: int, b: int, stepstop: int):
        """ optimized way of initializing a network, reducing timings, returns the list of nodes """
        if processes <= 0:
            processes = multiprocessing.cpu_count()
        # load balancing
        tasks = int(nodesize / processes)
        if nodesize % processes > 0:
            tasks += 1
        nodetasks = deque(maxlen=processes)
        nodes = deque(maxlen=nodesize)
        for t in range(processes):
            nodetasks.append(deque(maxlen=tasks))
        # init the network, but already keep the has if the id in memory (avoid having to do extra hashing)
        t, c = 0, 0
        for iditem in range(nodesize):
            clihash = Hash(iditem)
            dhtcli = DHTClient(iditem, self, bsize, a, b, stepstop)
            nodes.append((iditem, clihash))
            nodetasks[t].append(dhtcli)
            self.add_new_node(dhtcli)
            c += 1
            if c >= tasks:
                c = 0
                t += 1

        if processes <= 1:
            for cli in self.nodestore.nodes.values():
                optimalRTforDHTcli(cli, nodes, bsize)
        else:
            with ProcessPoolExecutor(max_workers=processes) as executor:
                inits = [executor.submit(self.parallel_clilist_initializer, nodelist, nodes, bsize) for nodelist in nodetasks]
                futures.wait(inits, return_when=futures.FIRST_EXCEPTION)
            for future in inits:
                clis = future.result()
                for cli in clis:
                    self.add_new_node(cli)
            # make sure that all clients have latest version of the network (necessary for high level of concurrency)
            for cli in self.nodestore.nodes.values():
                cli.network = self
        return self.nodestore.get_nodes()

    def add_new_node(self, newnode: DHTClient):
        """ add a new node to the DHT network """
        self.nodestore.add_node(newnode)

    def connect_to_node(self, ognode: int, targetnode: int):
        """ get connection to the DHTclient target from the PeerStore
         and an associated delay or raise an error """
        self.connectioncnt += 1
        try:
            # check the error rate (avoid stablishing the connection if there is an error)
            if random.randint(0, 99) < self.fasterrorrate:
                connerror = ConnectionError(targetnode, "fast error", time.time(), self.fastdelayrange)
                self.errortracker.append(connerror)
                raise connerror
            if random.randint(0, 99) < self.slowerrorrate:
                connerror = ConnectionError(targetnode, "slow error", time.time(), self.slowdelay)
                self.errortracker.append(connerror)
                raise connerror
            connection = Connection(self.connectioncnt, ognode, self.nodestore.get_node(targetnode), self.fastdelayrange)
            self.connectiontracker.append({
                'time': time.time(),
                'from': ognode,
                'to': targetnode,
                'delay': connection.delay})
            return connection, connection.delay

        except NodeNotInStoreError as e:
            connerror = ConnectionError(e.missingNode, e.description, e.time, 0)
            self.errortracker.append({
                'time': connerror.errorTime,
                'from': ognode,
                'error': connerror.description,
                'delay': 0})
            raise connerror

    def bootstrap_node(self, nodeid: int, bucketsize: int):  # ( accuracy: int = 100 )
        """ checks among all the existing nodes in the network, which are the correct ones to 
        fill up the routing table of the given node """
        # best way to know which nodes are the best nodes for a routing table, is to compose a rt itself 
        # Accuracy = How many closest peers / K closest peers do we know (https://github.com/plprobelab/network-measurements/blob/master/results/rfm19-dht-routing-table-health.md) 
        # TODO: generate a logic that selects the routing table with the given accuracy
        rt = RoutingTable(nodeid, bucketsize)
        for node in self.nodestore.get_nodes():
            if node == nodeid:
                continue
            rt.new_discovered_peer(node)
        return rt.get_routing_nodes()

    def summary(self):
        """ return the summary of what happened in the network """  
        return {
            'total_nodes': self.nodestore.len(),
            'attempts': self.connectioncnt,
            'successful': len(self.connectiontracker),
            'failures': len(self.errortracker)}

    def len(self) -> int:
        return self.nodestore.len()



