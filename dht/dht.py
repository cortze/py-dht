import os
import random
import time
import multiprocessing
from concurrent import futures
from concurrent.futures import ProcessPoolExecutor
from collections import deque, defaultdict, OrderedDict
from dht.key_store import KeyValueStore
from dht.routing_table import RoutingTable
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

    def lookup_for_hash(self, key: Hash, trackaccuracy: bool = False, finishwithfirstvalue: bool = True):
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

        origin_overhead = self.network.connection_overheads.get_overhead_for_node(self.ID)
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
            if finishwithfirstvalue and lookupvalue != "":
                break

            nodes = nodestotry.copy()
            for node in nodes:
                nodestotry.pop(node)  # remove item from peers to attempt
                if node in triednodes:  # make sure we don't contact the same node twice
                    continue
                triednodes.append(node)
                lookupsummary['connectionAttempts'] += 1
                remote_overhead = self.network.connection_overheads.get_overhead_for_node(node)
                try:
                    connection, conndelay = self.network.connect_to_node(self.ID, node, origin_overhead, remote_overhead)
                    newnodes, val, _, closestdelay = connection.get_closest_nodes_to(key)
                    # we only want to aggregate the difference between the base + conn delay - the already aggregated one
                    # this allows to simulate de delay of a proper scheduler
                    operationdelay = (conndelay + closestdelay)
                    if len(alpha_results) < self.alpha:
                        alpha_results.append((operationdelay, newnodes, val, origin_overhead + remote_overhead))
                        alpha_results = deque(sorted(alpha_results, key=lambda pair: pair[0]))
                    else:
                        print("huge error here")

                except ConnectionError as e:
                    errortype = e.error_type()
                    errordelay = e.get_delay()
                    alpha_results.append((errordelay, {}, "", origin_overhead + remote_overhead))
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
                        # only if the connection was successful, we modify the stepsCnt 
                        # conn failures don't count
                        if has_closer_nodes(closestnodes, mindelayedlookup[1]):
                            stepscnt = 0
                        else:
                            stepscnt += 1
                    else:
                        lookupsummary['failedCons'] += 1

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
            'accuracy': "unknown",
        })

        # limit the output to beta number of nodes
        closestnodes = OrderedDict(sorted(closestnodes.items(), key=lambda item: item[1])[:self.beta])

        # only check the accuracy if explicitly said
        if trackaccuracy:
            netclosestnodes = self.network.get_closest_nodes_to_hash(key, self.beta)
            oknodes = 0
            for nodeid in closestnodes:
                if (nodeid == netnode for netnode, _ in netclosestnodes):
                    oknodes += 1
            if oknodes == 0:
                lookupsummary["accuracy"] = 0
            else:
                lookupsummary["accuracy"] = int(oknodes/self.beta)*100

        # the aggregated delay of the operation is included with the summary `lookupsummary['aggrDelay']`
        return closestnodes, lookupvalue, lookupsummary, lookupsummary['aggrDelay']

    def get_closest_nodes_to(self, key: Hash):
        """ return the closest nodes to a given key from the local routing table (local perception of the network) """
        # check if we actually have the value of KeyValueStore, and return the content
        closernodes = self.rt.get_closest_nodes_to(key)
        val, ok = self.ks.read(key)
        return closernodes, val, ok

    def provide_block_segment(self, segment):
        """ looks for the closest nodes in the network, and sends them a """
        providesummary = {
            'succesNodeIDs': deque(),
            'failedNodeIDs': deque(),
            'startTime': time.time(),
        }
        segH = Hash(segment)
        closestnodes, _, lookupsummary, lookupdelay = self.lookup_for_hash(segH, finishwithfirstvalue=False)
        provAggrDelay = []
        for cn in closestnodes:
            origin_overhead = self.network.connection_overheads.get_overhead_for_node(self.ID)
            remote_overhead = self.network.connection_overheads.get_overhead_for_node(cn)
            try:
                connection, conndelay = self.network.connect_to_node(self.ID, cn, origin_overhead, remote_overhead)
                storedelay = connection.store_segment(segment)
                provAggrDelay.append(conndelay+storedelay)
                providesummary['succesNodeIDs'].append(cn)
            except ConnectionError as e:
                providesummary['failedNodeIDs'].append(cn)
                provAggrDelay.append(e.get_delay())

        provideDelay = max(provAggrDelay)
        providesummary.update({
            'contactedPeers': lookupsummary['connectionAttempts'],
            'closestNodes': closestnodes.keys(),
            'finishTime': time.time(),
            'lookupDelay': lookupdelay,
            'provideDelay': provideDelay,
            'operationDelay': lookupdelay+provideDelay,
        })
        return providesummary, providesummary['operationDelay']

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
        return "node_not_found"

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
    def __init__(self, err_id: int, f: int, to: int, error: str, delay, origin_overhead, remote_overhead):
        self.error_id = err_id
        self.f = f
        self.to = to
        self.error = error
        self.time = time.time()
        self.origin_overhead = origin_overhead
        self.remote_overhead = remote_overhead
        self.delay = delay
        self.total_overhead = origin_overhead + remote_overhead
        self.total_delay = self.delay + self.total_overhead
    
    def description(self) -> str:
        return f"unable to connect node {self.to} from {self.f}. {self.error}"

    def get_delay(self):
        return self.total_delay

    def error_type(self):
        return self.error

    def summary(self):
        return {
            'id': self.error_id,
            'time': self.time,
            'from': self.f,
            'to': self.to,
            'error': self.error,
            'base_delay': self.delay,
            'origin_overhead': self.origin_overhead,
            'remote_overhead': self.remote_overhead,
            'total_overhead': self.total_overhead,
            'total_delay': self.total_delay,
        }


class Connection:
    """ connection simbolizes the interaction that 2 DHTClients could have with each other """
    def __init__(self, conn_id: int, f: int, to: DHTClient, delay, originoverhead, remoteoverhead):
        self.conn_id = conn_id
        self.time = time.time()
        self.f = f
        self.to = to
        self.delay = delay
        self.origin_overhead = originoverhead
        self.remote_overhead = remoteoverhead
        self.total_overhead = originoverhead + remoteoverhead
        self.total_delay = delay + self.total_overhead

    def get_closest_nodes_to(self, key: Hash):
        closer_nodes, val, ok = self.to.get_closest_nodes_to(key)
        return closer_nodes, val, ok, self.total_delay

    def store_segment(self, segment):
        self.to.store_segment(segment)
        return self.total_delay

    def retrieve_segment(self, key: Hash):
        seg, ok = self.to.retrieve_segment(key)
        return seg, ok, self.total_delay

    def summary(self):
        return {
            'id': self.conn_id,
            'time': self.time,
            'from': self.f,
            'to': self.to.ID,
            'error': "None",
            'base_delay': self.delay,
            'origin_overhead': self.origin_overhead,
            'remote_overhead': self.remote_overhead,
            'total_overhead': self.total_overhead,
            'total_delay': self.total_delay,
        }


class OverheadTracker:
    """keeps tracks of the overhead for each node in the network, which will be increased
    after a connection is established. This overhead will be added to each of the operations
    until the overhead is reset (finishing the end of the concurren operations)"""
    def __init__(self, gamma_overhead: float):
        self.gamma_overhead = gamma_overhead
        self.nodes = defaultdict(int)

    def get_overhead_for_node(self, node_id: int):
        """returns and increases the overhead for the given peer"""
        try:
            overhead = self.nodes[node_id] + self.gamma_overhead
        except KeyError:
            overhead = 0
        self.nodes[node_id] = overhead
        return overhead

    def reset_overhead_for_node(self, node_id: int):
        del self.nodes[node_id]

    def reset_overheads(self):
        self.nodes = defaultdict(int)


class DHTNetwork:
    """ serves a the shared point between all the nodes participating in the simulation,
    allows node to communicat with eachother without needing to implement an API or similar"""

    def __init__(self, networkid: int, fasterrorrate: int=0, slowerrorrate: int=0, conndelayrange = None, fastdelayrange = None, slowdelayrange = None, gammaoverhead: float = 0.0):
        """ class initializer, it allows to define the networkID and the delays between nodes """
        self.networkid = networkid
        self.fasterrorrate = fasterrorrate  # %
        self.slowerrorrate = slowerrorrate  # %
        self.conn_delay_range = conndelayrange
        self.fast_delay_range = fastdelayrange  # list() in ms -> i.e., (5, 100) ms | None
        self.slow_delay_range = slowdelayrange  # list() in ms -> i.e., (5, 100) ms | None
        self.nodestore = NodeStore()
        self.error_tracker = deque()  # every time that an error is tracked, add it to the queue
        self.connection_tracker = deque()  # every time that a connection was established
        self.connection_overheads = OverheadTracker(gammaoverhead)
        self.connectioncnt = 0

    def get_closest_nodes_to_hash(self, target: Hash, beta):
        closestnodes = deque(maxlen=self.nodestore.len())
        for cliid, cli in self.nodestore.nodes.items():
            dist = cli.hash.xor_to_hash(target)
            closestnodes.append((cliid, dist))
        return sorted(closestnodes, key=lambda dist: dist[1])[:beta]

    def optimal_rt_for_dht_cli(self, dhtcli, nodes, bucketsize):
        idsanddistperbucket = deque()
        for nodeid, nodehash in nodes:
            if nodeid == dhtcli.ID:
                continue
            sbits = dhtcli.hash.shared_upper_bits(nodehash)
            dist = dhtcli.hash.xor_to_hash(nodehash)
            while len(idsanddistperbucket) < sbits + 1:
                idsanddistperbucket.append(deque())
            idsanddistperbucket[sbits].append((nodeid, dist))
        for b in idsanddistperbucket:
            for iddist in sorted(b, key=lambda pair: pair[1])[:bucketsize]:
                dhtcli.rt.new_discovered_peer(iddist[0])
        return dhtcli

    def parallel_clilist_initializer(self, clilist, nodes, k):
        clis = deque(maxlen=len(clilist))
        for cli in clilist:
            clis.append(self.optimal_rt_for_dht_cli(cli, nodes, k))
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
                self.optimal_rt_for_dht_cli(cli, nodes, bsize)
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

    def connect_to_node(self, ognode: int, targetnode: int, originoverhead: float = 0.0, remoteoverhead: float = 0.0):
        """ get connection to the DHTclient target from the PeerStore
         and an associated delay or raise an error """
        self.connectioncnt += 1
        def get_delay_from_range(range):
            if range == None:
                delay = 0
            else:
                delay = random.sample(range, 1)[0]
            return delay
        conn_delay = get_delay_from_range(self.conn_delay_range)
        fast_delay = get_delay_from_range(self.fast_delay_range)
        slow_delay = get_delay_from_range(self.slow_delay_range)
        try:
            # check the error rate (avoid stablishing the connection if there is an error)
            if random.randint(0, 99) < self.fasterrorrate:
                conn_error = ConnectionError(self.connectioncnt, ognode, targetnode, "fast", fast_delay, originoverhead, remoteoverhead)
                self.error_tracker.append(conn_error.summary())
                raise conn_error
            if random.randint(0, 99) < self.slowerrorrate:
                conn_error = ConnectionError(self.connectioncnt, ognode, targetnode, "slow", slow_delay, originoverhead, remoteoverhead)
                self.error_tracker.append(conn_error.summary())
                raise conn_error
            connection = Connection(self.connectioncnt, ognode, self.nodestore.get_node(targetnode), conn_delay, originoverhead, remoteoverhead)
            self.connection_tracker.append(connection.summary())
            return connection, connection.delay

        except NodeNotInStoreError:
            conn_error = ConnectionError(self.connectioncnt, ognode, targetnode, "node_not_found", slow_delay, originoverhead, remoteoverhead)
            self.error_tracker.append(conn_error.summary())
            raise conn_error

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

    def reset_network_metrics(self):
        """reset the connection tracker and the overhead, emulates the end of concurrent operations"""
        self.error_tracker = deque()
        self.connection_tracker = deque()
        self.connection_overheads.reset_overheads()

    def summary(self):
        """ return the summary of what happened in the network """
        return {
            'total_nodes': self.nodestore.len(),
            'attempts': self.connectioncnt,
            'successful': len(self.connection_tracker),
            'failures': len(self.error_tracker)}

    def connection_metrics(self):
        """aggregate all the connection and errors into a single dict -> easily translatable to panda.df"""
        network_metrics = {
            'conn_id': [],
            'time': [],
            'from': [],
            'to': [],
            'error': [],
            'base_delay': [],
            'origin_overhead': [],
            'remote_overhead': [],
            'total_overhead': [],
            'final_delay': [],
        }
        for conn in (self.connection_tracker + self.error_tracker):
            network_metrics['conn_id'].append(conn['id'])
            network_metrics['time'].append(conn['time'])
            network_metrics['from'].append(conn['from'])
            network_metrics['to'].append(conn['to'])
            network_metrics['error'].append(conn['error'])
            network_metrics['base_delay'].append(conn['base_delay'])
            network_metrics['origin_overhead'].append(conn['origin_overhead'])
            network_metrics['remote_overhead'].append(conn['remote_overhead'])
            network_metrics['total_overhead'].append(conn['total_overhead'])
            network_metrics['total_delay'].append(conn['total_delay'])
        return network_metrics


    def len(self) -> int:
        return self.nodestore.len()

