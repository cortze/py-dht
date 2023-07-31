from ctypes import sizeof
from dht.hashes import Hash
from collections import deque, defaultdict, OrderedDict


class RoutingTable:
    def __init__(self, localnodeid:int, bucketsize:int) -> None:
        self.localnodeid = localnodeid
        self.bucketsize = bucketsize
        self.kbuckets = deque()
        self.lastupdated = 0  # not really used at this time

    def new_discovered_peer(self, nodeid:int):
        """ notify the routing table of a new discovered node
        in the network and check if it has a place in a given bucket """
        # check matching bits
        localnodehash = Hash(self.localnodeid)
        nodehash = Hash(nodeid)
        sbits = localnodehash.shared_upper_bits(nodehash)
        # Check if there is a kbucket already at that place
        while len(self.kbuckets) < sbits+1:
            # Fill middle kbuckets if needed
            self.kbuckets.append(KBucket(self.localnodeid, self.bucketsize))
        # check/update the bucket with the newest nodeID
        self.kbuckets[sbits] = self.kbuckets[sbits].add_peer_to_bucket(nodeid)
        return self

    def get_closest_nodes_to(self, key: Hash):
        """ return the list of Nodes (in order) close to the given key in the routing table """
        closestnodes = defaultdict()
        # check the distances for all the nodes in the rt
        for b in self.kbuckets:
            for n in b.bucketnodes:
                nH = Hash(n)
                dist = nH.xor_to_hash(key)
                closestnodes[n] = dist
        # sort the dict based on dist 
        closestnodes = OrderedDict(sorted(closestnodes.items(), key=lambda item: item[1])[:self.bucketsize])
        return closestnodes

    def get_routing_nodes(self):
        # get the closest nodes to the peer
        rtnodes = deque()
        for b in self.kbuckets:
            for n in b.bucketnodes:
                rtnodes.append(n)
        return rtnodes

    def __repr__(self) -> str:
        s = "" 
        for i, b in enumerate(self.kbuckets):
            s += f"b{i}:{len(b)} "
        return s
   
    def summary(self) -> str:
        return self.__repr__()


class KBucket:
    """ single representation of a kademlia kbucket, which contains the closest nodes
    sharing X number of upper bits on their NodeID's Hashes """

    def __init__(self, localnodeid: int, size: int):
        """ initialize the kbucket with setting a max size along some other control variables """
        self.localnodeid = localnodeid
        self.localnodehash = Hash(localnodeid)
        self.bucketnodes = deque(maxlen=size)
        self.bucketsize = size
        self.lastupdated = 0

    def add_peer_to_bucket(self, nodeid: int):
        """ check if the new node is elegible to replace a further one """
        nodehash = Hash(nodeid)
        dist = self.localnodehash.xor_to_hash(nodehash)
        bucketdistances = self.get_distances_to_key(self.localnodehash)
        if len(self) >= self.bucketsize:
            maxval = max(bucketdistances)
            if maxval < dist:
                pass
            else:
                maxvalid = bucketdistances.index(maxval)
                del self.bucketnodes[maxvalid]
                self.bucketnodes.append(nodeid)
        else:
            self.bucketnodes.append(nodeid)
        return self

    def get_distances_to_key(self, key: Hash):
        """ return the distances from all the nodes in the bucket to a given key """
        distances = deque()
        for nodeid in self.bucketnodes:
            nodehash = Hash(nodeid)
            dist = nodehash.xor_to_hash(key)
            distances.append(dist)
        return distances

    def get_bucket_nodes(self):
        return self.bucketnodes.copy()

    def __len__(self) -> int:
        return len(self.bucketnodes)

    def __repr__(self) -> str:
        return f"{len(self)} nodes"





