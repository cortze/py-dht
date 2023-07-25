import ctypes

# TODO: swapt hash to SHA256 with the possibility of reusing a given seed for reproducibility
# at the moment, I'm using the default 64bit hash function from Python
HASH_BASE = 64

class Hash():
    def __init__(self, value):
        """ basic representation of a Hash object for the DHT, which includes the main utilities related to a hash """
        self.value = self.hash_key(value)
        self.bitArray = BitArray(self.value, HASH_BASE)
        # TODO: the hash values could be reproduced if the ENVIRONMENT VARIABLE PYTHONHASHSEED is set to a 64 bit integer https://docs.python.org/3/using/cmdline.html#envvar-PYTHONHASHSEED

    def hash_key(self, key):
        """ creates a hash value for the given Key """
        # If the key is a plain integer, use the hex encoding to generate more entropy on the hash
        if isinstance(key, int):
            key = hex(key)
        h = hash(key)
        # ensure that the hash is unsigned
        return ctypes.c_ulong(h).value
 
    def xor_to(self, targetHash:int) -> int:
        """ Returns the XOR distance between both hash values"""
        distance = self.value ^ targetHash
        return ctypes.c_ulong(distance).value

    def xor_to_hash(self, targetHash) -> int:
        """ Returns the XOR distance between both hash values"""
        distance = self.value ^ targetHash.value
        return ctypes.c_ulong(distance).value

    def shared_upper_bits(self, targetHash) -> int:
        """ returns the number of upper sharing bits between 2 hash values """
        targetBits = BitArray(targetHash.value, HASH_BASE)
        sBits = self.bitArray.upper_sharing_bits(targetBits)
        return sBits

    def __repr__(self) -> str:
        return str(hex(self.value))

    def __eq__(self, targetHash) -> bool:
        return self.value == targetHash.value

    def is_smaller_than(self, targetHash) -> bool:
        return self.value < targetHash.value 

    def is_greater_than(self, targetHash) -> bool:
        return self.value > targetHash.value 

class BitArray():
    """ array representation of an integer using only bits, ideal for finding matching upper bits"""
    def __init__(self, intValue:int, base:int):
        self.base = base
        self.bitArray = self.bin(intValue)

    def __repr__(self):
        return str(self.bitArray)

    def upper_sharing_bits(self, targetBitArray) -> int:
        sBits = 0
        for i, bit in enumerate(self.bitArray):
            if bit == targetBitArray.get_x_bit(i):
                sBits += 1
            else:
                break 
        return sBits

    def get_x_bit(self, idx:int = 0):
        return self.bitArray[idx]

    def bin(self, n):
        s = ""
        i = 1 << self.base-1
        while(i > 0) :
            if((n & i) != 0) :
                s += "1"
            else :
                s += "0"
            i = i // 2
        return s
