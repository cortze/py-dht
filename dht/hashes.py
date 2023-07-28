import ctypes
from bitarray.util import int2ba

# TODO: swapt hash to SHA256 with the possibility of reusing a given seed for reproducibility
# at the moment, I'm using the default 64bit hash function from Python
HASH_BASE = 64


class Hash:
    def __init__(self, value):
        """ basic representation of a Hash object for the DHT, which includes the main utilities related to a hash """
        self.value = self.hash_key(value)
        self.bitarray = BitArray(self.value, HASH_BASE)
        # TODO: the hash values could be reproduced if the ENVIRONMENT VARIABLE PYTHONHASHSEED is set to a 64 bit integer https://docs.python.org/3/using/cmdline.html#envvar-PYTHONHASHSEED

    def hash_key(self, key):
        """ creates a hash value for the given Key """
        # If the key is a plain integer, use the hex encoding to generate more entropy on the hash
        if isinstance(key, int):
            key = hex(key)
        h = hash(key)
        # ensure that the hash is unsigned
        return ctypes.c_ulong(h).value
 
    def xor_to(self, targetint: int) -> int:
        """ Returns the XOR distance between both hash values"""
        return ctypes.c_ulong(self.value ^ targetint).value

    def xor_to_hash(self, targethash) -> int:
        """ Returns the XOR distance between both hash values"""
        return ctypes.c_ulong(self.value ^ targethash.value).value

    def shared_upper_bits(self, targethash) -> int:
        """ returns the number of upper sharing bits between 2 hash values """
        return self.bitarray.upper_sharing_bits(targethash.bitarray)

    def __repr__(self) -> str:
        return str(hex(self.value))

    def __eq__(self, targethash) -> bool:
        return self.value == targethash.value


class BitArray:
    """ array representation of an integer using only bits, ideal for finding matching upper bits"""
    def __init__(self, uintval:int, base:int):
        self.base = base
        self.bitarray = int2ba(uintval, length=base)

    def __repr__(self):
        return self.bitarray.to01()

    def upper_sharing_bits(self, targetba) -> int:
        sbits = 0
        proc = self.bitarray ^ targetba.bitarray
        for bit in proc:
            if bit == 0:
                sbits += 1
            else:
                break 
        return sbits

