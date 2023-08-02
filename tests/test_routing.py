import unittest
from dht.routing_table import RoutingTable, KBucket
from dht.hashes import Hash

class TestDHTHashes(unittest.TestCase):
    def test_kbucket(self):
        bucketsize = 4
        localid = 1
        localhash = Hash(localid)
        remoteids = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]

        kbucket = KBucket(localid, bucketsize)
        ogs = []
        for id in remoteids:
            h = Hash(id)
            ogs.append(localhash.xor_to_hash(h))
            kbucket.add_peer_to_bucket(id)

        # Firts, check the bucket didn't go beyond the size
        self.assertEqual(len(kbucket), bucketsize)
        zippedogs = zip(remoteids, ogs)
        zippedogs = sorted(zippedogs, key=lambda pair: pair[1])
        # Second, ensure that the bucket has in fact the closest nodeIDs to the local ID
        distances = kbucket.get_distances_to_key(localhash)
        ids = kbucket.get_bucket_nodes()
        zipped_b = sorted(zip(ids, distances), key=lambda pair: pair[1])
        for idx, pair in enumerate(zipped_b):
            self.assertEqual(zippedogs[idx], pair)

    def test_routing_table(self):
        totalnodes = 700
        bucketsize = 5
        localid = 1
        localhash = Hash(localid)
        remoteids = range(localid+1, 1+totalnodes, 1)

        rt = RoutingTable(localid, bucketsize)

        bucketcontrol = {}
        sharedbits = {}
        distances = []
        for id in remoteids:
            idH = Hash(id)
            sBits = localhash.shared_upper_bits(idH)
            dist = localhash.xor_to_hash(idH)
            if sBits not in bucketcontrol.keys():
                bucketcontrol[sBits] = 0
            bucketcontrol[sBits] += 1
            sharedbits[id] = sBits
            rt.new_discovered_peer(id)
            distances.append(dist) 

        # check that there is no missing bucket, and that max item per buckets is maintained
        for i, b in enumerate(rt.kbuckets):
            if i in bucketcontrol:
                if bucketcontrol[i] > bucketsize:
                    self.assertEqual(len(b), bucketsize)
                else:
                    self.assertEqual(len(b), bucketcontrol[i])
            else:
                self.assertEqual(len(b), 0)

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

        closestnodes = rt.get_closest_nodes_to(randomhash)

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


