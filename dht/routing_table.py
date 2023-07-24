from ctypes import sizeof
from dht.hashes import Hash 

class RoutingTable():

    def __init__(self, localNodeID:int, bucketSize:int) -> None:
        self.localNodeID = localNodeID
        self.bucketSize = bucketSize
        self.kbuckets = []
        self.lastUpdated = 0

    def newDiscoveredPeer(self, nodeID:int):
        """ notify the routing table of a new discovered node
        in the network and check if it has a place in a given bucket """
        # check matching bits
        localNodeH = Hash(self.localNodeID)
        nodeH = Hash(nodeID)
        sBits = localNodeH.sharedUpperBits(nodeH)
        # Check if there is a kbucket already at that place
        while len(self.kbuckets) < sBits+1:
            # Fill middle kbuckets if needed
            self.kbuckets.append(KBucket(self.localNodeID, self.bucketSize))
        # check/update the bucket with the newest nodeID
        self.kbuckets[sBits] = self.kbuckets[sBits].addPeerToBucket(nodeID)
        return self

    def getClosestNodesTo(self, key:Hash):
        """ return the list of Nodes (in order) close to the given key in the routing table """
        closestNodes = {}
        # check the distances for all the nodes in the rt
        for b in self.kbuckets:
            for n in b.bucketNodes:
                nH = Hash(n)
                dist = nH.xorToHash(key)
                closestNodes[n] = dist
        # sort the dict based on dist 
        closestNodes = dict(sorted(closestNodes.items(), key=lambda item: item[1])[:self.bucketSize-1])
        return closestNodes

    def __repr__(self) -> str:
        s = "" 
        for i, b in enumerate(self.kbuckets):
            s += f"b{i}:{b.len()} "
        return s
    

class KBucket():
    """ single representation of a kademlia kbucket, which contains the closest nodes
    sharing X number of upper bits on their NodeID's Hashes """

    def __init__(self, ourNodeID:int, size:int):
        """ initialize the kbucket with setting a max size along some other control variables """
        self.localNodeID = ourNodeID
        self.bucketNodes = []
        self.bucketSize = size
        self.lastUpdated = 0

    def addPeerToBucket(self, nodeID:int):
        """ check if the new node is elegible to replace a further one """
        # Check if the distance between our NodeID and the remote one
        localNodeH = Hash(self.localNodeID)
        nodeH = Hash(nodeID)
        dist = localNodeH.xorToHash(nodeH)

        bucketDistances = self.getDistancesToKey(localNodeH)
        if (self.len() > 0) and (self.len() >= self.bucketSize):
            if bucketDistances[list(bucketDistances)[-1]] < dist:
                    pass
            else:
                # As the dist of the new node is smaller, add it to the list
                bucketDistances[nodeID] = dist
                # Sort back the nodes with the new one and remove the last remaining item
                bucketDistances = dict(sorted(bucketDistances.items(), key=lambda item: item[1]))
                bucketDistances.pop(list(bucketDistances)[-1])
                # Update the new closest nodes in the bucket
                self.bucketNodes = list(bucketDistances.keys())
        else: 
            self.bucketNodes.append(nodeID)
        return self
   
    def getDistancesToKey(self, key:Hash):
        """ return the distances from all the nodes in the bucket to a given key """
        distances = {}
        for nodeID in self.bucketNodes:
            nodeH = Hash(nodeID)
            dist = nodeH.xorToHash(key)
            distances[nodeID] = dist
        return dict(sorted(distances.items(), key=lambda item: item[1]))

    def getXNodesCloseTo(self, key:Hash, numberOfNodes:int):
        """ return the XX number of nodes close to a key from this bucket """
        print(f"checking in bucket {numberOfNodes} nodes")
        distances = {}
        if numberOfNodes <= 0:
            return distances
        distances = self.getDistancesToKey(key) 
        # Get only the necessary and closest nodes to the key from the kbucket
        nodes = {}
        for node, dist in distances.items():
            nodes[node] = dist
            if len(nodes) <= numberOfNodes:
                break
        return nodes 
   
    def len(self) -> int:
        return len(self.bucketNodes)

    def __repr__(self) -> str:
        return f"{self.len()} nodes"




