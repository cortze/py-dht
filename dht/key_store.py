from dht.hashes import Hash

class KeyValueStore():
    """ Memory-storage unit that will keep track of each of the key-values that a DHT client has to keep locally""" 

    def __init__(self):
        """ compose the storage unit in memory """
        self.storage = {}

    def add(self, key:Hash, value):
        """ aggregates a new value to the store, or overrides it if it was already a value for the key """
        self.storage[key.value] = value

    def remove(self, key:Hash):
        self.storage.pop(key.value)

    def read(self, key:Hash):
        """ reads a value for the given Key, or return false if it wasn't found """
        value, ok = self.storage[key.value]
        return value, ok 

    def summary(self) -> int:
        """ returns the number of items stored in the local KeyValueStore """
        return len(self.storage) 
