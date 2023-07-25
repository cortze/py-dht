import unittest
import random
import ctypes
from dht.hashes import Hash, BitArray

class TestDHTHashes(unittest.TestCase):

    def test_int_hashing(self):
        # get randome "validator_indexes"
        ids = random.choices(range(1, 100), k=10)
        for id in ids:
            hash_obj = Hash(id)
            plain_hash = ctypes.c_ulong(hash(hex(id))).value
            self.assertEqual(hash_obj.value, plain_hash)

    def test_xor_distances(self):
        # get randome "validator_indexes"
        ids = random.choices(range(1, 100), k=10)
        for id in ids:
            hash_obj = Hash(id)
            plain_hash = ctypes.c_ulong(hash(hex(id))).value
            distance = hash_obj.xor_to(plain_hash)
            self.assertEqual(distance, 0)
        for id in ids:
            hash_obj = Hash(id)
            plain_hash = ctypes.c_ulong(hash(hex(id+1))).value
            distance = hash_obj.xor_to(plain_hash)
            self.assertNotEqual(distance, 0)

    def test_bit_array(self):
        bitArray_1 = BitArray(1, 4)
        bitArray_2 = BitArray(2, 4)
        bitArray_3 = BitArray(4, 4)
        bitArray_4 = BitArray(8, 4) 

        # Checks
        self.assertEqual(bitArray_1.upper_sharing_bits(bitArray_1), 4)
        self.assertEqual(bitArray_1.upper_sharing_bits(bitArray_2), 2)
        self.assertEqual(bitArray_1.upper_sharing_bits(bitArray_3), 1)
        self.assertEqual(bitArray_1.upper_sharing_bits(bitArray_4), 0)

        self.assertEqual(bitArray_2.upper_sharing_bits(bitArray_2), 4)
        self.assertEqual(bitArray_2.upper_sharing_bits(bitArray_3), 1)
        self.assertEqual(bitArray_2.upper_sharing_bits(bitArray_4), 0)
        
        self.assertEqual(bitArray_3.upper_sharing_bits(bitArray_3), 4)
        self.assertEqual(bitArray_3.upper_sharing_bits(bitArray_4), 0)

        self.assertEqual(bitArray_4.upper_sharing_bits(bitArray_4), 4)

if __name__ == '__main__':
    unittest.main()


def simple_visualization():
    # Small introduction to the hashing and ctypes before testing
    val_id = 1
    val_id_hash = hash(hex(val_id))
    val_id_hash_ctype = ctypes.c_ulong(val_id_hash) 

    print(f"val {val_id} hash (c_t) -> ", val_id_hash_ctype.value)
    print(f"val {val_id} hash (bin) -> ", bin(val_id_hash_ctype.value))

    val_id_2 = 2
    val_id_hash_2 = hash(hex(val_id_2))
    val_id_hash_ctype_2 = ctypes.c_ulong(val_id_hash_2) 

    print(f"val {val_id_2} hash (c_t) -> ", val_id_hash_ctype_2.value)
    print(f"val {val_id_2} hash (bin) -> ", bin(val_id_hash_ctype_2.value))

    distance = val_id_hash_ctype.value ^ val_id_hash_ctype_2.value
    print("XOR (int): ", distance)
    print("XOR (bin): ", bin(distance))
