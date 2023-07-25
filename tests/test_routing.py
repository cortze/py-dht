import unittest
from dht.routing_table import RoutingTable, KBucket
from dht.hashes import Hash

class TestDHTHashes(unittest.TestCase):
    def test_kbucket(self):
        bucketSize = 2
        localID = 1
        localH = Hash(localID)
        remoteIDs = [2, 3, 4]
        
        kbucket = KBucket(localID, bucketSize)
        ogDistances = []
        for id in remoteIDs:
            h = Hash(id)
            ogDistances.append(localH.xor_to_hash(h))
            kbucket.add_peer_to_bucket(id)
        
        # Firts, check the bucket didn't go beyond the size
        self.assertEqual(kbucket.len(), bucketSize)

        distances = ogDistances.copy()
        # Second, ensure that the bucket has in fact the closest nodeIDs to the local ID
        closeNodesInBucket = kbucket.get_x_nodes_close_to(localH, bucketSize)
        for node in closeNodesInBucket:
            minDist = min(distances)
            minIdx = get_index_of_value(ogDistances, minDist)
            self.assertEqual(node, remoteIDs[minIdx])
            distances = remove_item_from_array(distances, get_index_of_value(distances, minDist))

    def test_routing_table(self):
        totalNodes = 40
        bucketSize = 5
        localID = 1
        localH = Hash(localID)
        remoteIDs = range(localID+1, localID+1+totalNodes, 1)

        routingTable = RoutingTable(localID, bucketSize)

        bucketControl = {}
        sharedBits = {}
        distances = []
        for id in remoteIDs:
            idH = Hash(id)
            sBits = localH.shared_upper_bits(idH)
            dist = localH.xor_to_hash(idH)
            if sBits not in bucketControl.keys():
                bucketControl[sBits] = 0
            bucketControl[sBits] += 1
            sharedBits[id] = sBits
            routingTable.new_discovered_peer(id)
            distances.append(dist) 
      
#       print("summary of the rt:")
#       print(routingTable)
#       print(bucketControl)
#       print(sharedBits)
#       print(distances,'\n')

        # check that there is no missing bucket, and that max item per buckets is maintained
        for i, b in enumerate(routingTable.kbuckets):
            if i in bucketControl:
                if bucketControl[i] > bucketSize:
                    self.assertEqual(b.len(), bucketSize)
                else: 
                    self.assertEqual(b.len(), bucketControl[i])
            else:
                self.assertEqual(b.len(), 0)

        randomID = totalNodes + 100
        randomH = Hash(randomID)
        
        rtNodes = []
        distanceToHash = []
        sharedBits = {}
        # the loookup in the routing table will give us the nodes IN the rt with the least distance to hashes
        # Thus, only compare it with the IDs in the routing table
        for b in routingTable.kbuckets:
            for node in b.bucketNodes:
                nodeH = Hash(node)
                sharedBits[node] = nodeH.shared_upper_bits(randomH)
                distanceToHash.append(nodeH.xor_to_hash(randomH))
                rtNodes.append(node)

#       print("summary of the lookup:")
#       print(localH.sharedUpperBits(randomH))
#       print(sharedBits)
#       print(rtNodes)
#       print(distanceToHash,'\n')

        closestNodes = routingTable.get_closest_nodes_to(randomH)
#       print(closestNodes)

        # check if the closest nodes are actually the closest ones in the rt
        distances_copy = distanceToHash.copy()
        for node in closestNodes:
            minDist = min(distances_copy)
            minDistNode = rtNodes[get_index_of_value(distanceToHash, minDist)]
            self.assertEqual(node, minDistNode)
            distances_copy = remove_item_from_array(distances_copy, get_index_of_value(distances_copy,minDist))


def get_index_of_value(array, value):
    return array.index(value)

def remove_item_from_array(array, item):
    del array[item]
    return array 

if __name__ == '__main__':
    unittest.main()


