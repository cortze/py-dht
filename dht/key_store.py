from dht.hashes import Hash
from collections import defaultdict


class KeyValueStore:
    """ Memory-storage unit that will keep track of each of the key-values that a DHT client has to keep locally""" 

    def __init__(self):
        """ compose the storage unit in memory """
        self.storage = defaultdict()

    def add(self, key: Hash, value):
        """ aggregates a new value to the store, or overrides it if it was already a value for the key """
        self.storage[key.value] = value

    def remove(self, key: Hash):
        self.storage.pop(key.value)

    def read(self, key: Hash):
        """ reads a value for the given Key, or return false if it wasn't found """
        try: 
            value = self.storage[key.value]
            ok = True
        except KeyError:
            value = ""
            ok = False
        return value, ok 

    def __len__(self):
        return len(self.storage)
