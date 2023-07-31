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
            s += f"b{i}:{b.len()} "
        return s
   
    def summary(self) -> str:
        return self.__repr__()


class KBucket:
    """ single representation of a kademlia kbucket, which contains the closest nodes
    sharing X number of upper bits on their NodeID's Hashes """

    def __init__(self, localnodeid: int, size: int):
        """ initialize the kbucket with setting a max size along some other control variables """
        self.localnodeid = localnodeid
        self.bucketnodes = deque(maxlen=size)
        self.bucketsize = size
        self.lastupdated = 0

    def add_peer_to_bucket(self, nodeid: int):
        """ check if the new node is elegible to replace a further one """
        # Check if the distance between our NodeID and the remote one
        localnodehash = Hash(self.localnodeid)
        nodehash = Hash(nodeid)
        dist = localnodehash.xor_to_hash(nodehash)
        bucketdistances = self.get_distances_to_key(localnodehash)
        if (self.len() > 0) and (self.len() >= self.bucketsize):
            if bucketdistances[deque(bucketdistances)[-1]] < dist:
                pass
            else:
                # As the dist of the new node is smaller, add it to the list
                bucketdistances[nodeid] = dist
                # Sort back the nodes with the new one and remove the last remaining item
                bucketdistances = OrderedDict(sorted(bucketdistances.items(), key=lambda item: item[1]))
                bucketdistances.pop(deque(bucketdistances)[-1])
                # Update the new closest nodes in the bucket
                self.bucketnodes = deque(bucketdistances.keys(), maxlen=len(bucketdistances))
        else: 
            self.bucketnodes.append(nodeid)
        return self
   
    def get_distances_to_key(self, key: Hash):
        """ return the distances from all the nodes in the bucket to a given key """
        distances = defaultdict()
        for nodeid in self.bucketnodes:
            nodehash = Hash(nodeid)
            dist = nodehash.xor_to_hash(key)
            distances[nodeid] = dist
        return OrderedDict(sorted(distances.items(), key=lambda item: item[1]))

    def get_x_nodes_close_to(self, key: Hash, nnodes: int):
        """ return the XX number of nodes close to a key from this bucket """
        print(f"checking in bucket {nnodes} nodes")
        distances = self.get_distances_to_key(key)
        # Get only the necessary and closest nodes to the key from the kbucket
        for i, _ in list(distances.keys())[nnodes:]:  # rely on std array, as the size is small and it can be sliced :)
            distances.pop(i)
        return distances
   
    def len(self) -> int:
        return len(self.bucketnodes)

    def __len__(self) -> int:
        return len(self.bucketnodes)

    def __repr__(self) -> str:
        return f"{self.len()} nodes"





