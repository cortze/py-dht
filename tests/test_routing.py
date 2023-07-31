import unittest
from dht.routing_table import RoutingTable, KBucket
from dht.hashes import Hash

class TestDHTHashes(unittest.TestCase):
    def test_kbucket(self):
        bucketsize = 2
        localid = 1
        localhash = Hash(localid)
        remoteids = [2, 3, 4]
        
        kbucket = KBucket(localid, bucketsize)
        ogs = []
        for id in remoteids:
            h = Hash(id)
            ogs.append(localhash.xor_to_hash(h))
            kbucket.add_peer_to_bucket(id)
        
        # Firts, check the bucket didn't go beyond the size
        self.assertEqual(len(kbucket), bucketsize)

        distances = ogs.copy()
        # Second, ensure that the bucket has in fact the closest nodeIDs to the local ID
        closeNodesInBucket = kbucket.get_x_nodes_close_to(localhash, bucketsize)
        for node in closeNodesInBucket:
            minDist = min(distances)
            minIdx = get_index_of_value(ogs, minDist)
            self.assertEqual(node, remoteids[minIdx])
            distances = remove_item_from_array(distances, get_index_of_value(distances, minDist))

    def test_routing_table(self):
        totalnodes = 40
        bucketsize = 5
        localid = 1
        localhash = Hash(localid)
        remoteids = range(localid+1, localid+1+totalnodes, 1)

        rt = RoutingTable(localid, bucketsize)

        bucketcontrol = {}
        sbits = {}
        distances = []
        for id in remoteids:
            idH = Hash(id)
            sBits = localhash.shared_upper_bits(idH)
            dist = localhash.xor_to_hash(idH)
            if sBits not in bucketcontrol.keys():
                bucketcontrol[sBits] = 0
            bucketcontrol[sBits] += 1
            sbits[id] = sBits
            rt.new_discovered_peer(id)
            distances.append(dist) 
      
#       print("summary of the rt:")
#       print(routingTable)
#       print(bucketControl)
#       print(sharedBits)
#       print(distances,'\n')

        # check that there is no missing bucket, and that max item per buckets is maintained
        for i, b in enumerate(rt.kbuckets):
            if i in bucketcontrol:
                if bucketcontrol[i] > bucketsize:
                    self.assertEqual(b.len(), bucketsize)
                else:
                    self.assertEqual(b.len(), bucketcontrol[i])
            else:
                self.assertEqual(b.len(), 0)

        randomid = totalnodes + 100
        randomhash = Hash(randomid)
        
        rtnodes = []
        distancetohash = []
        sbits = {}
        # the loookup in the routing table will give us the nodes IN the rt with the least distance to hashes
        # Thus, only compare it with the IDs in the routing table
        for b in rt.kbuckets:
            for node in b.bucketnodes:
                nodehash = Hash(node)
                sbits[node] = nodehash.shared_upper_bits(randomhash)
                distancetohash.append(nodehash.xor_to_hash(randomhash))
                rtnodes.append(node)

#       print("summary of the lookup:")
#       print(localH.sharedUpperBits(randomH))
#       print(sharedBits)
#       print(rtNodes)
#       print(distanceToHash,'\n')

        closestnodes = rt.get_closest_nodes_to(randomhash)
#       print(closestNodes)

        # check if the closest nodes are actually the closest ones in the rt
        distances_copy = distancetohash.copy()
        for node in closestnodes:
            mindist = min(distances_copy)
            mindistnode = rtnodes[get_index_of_value(distancetohash, mindist)]
            self.assertEqual(node, mindistnode)
            distances_copy = remove_item_from_array(distances_copy, get_index_of_value(distances_copy, mindist))


def get_index_of_value(array, value):
    return array.index(value)

def remove_item_from_array(array, item):
    del array[item]
    return array 

if __name__ == '__main__':
    unittest.main()


