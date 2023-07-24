import time
from dht.dht import DHTClient
from dht.node_store import NodeStore, NodeNotInStore

class ConnectionError(Exception):
    """ custom connection error exection to notify an errored connection """ 
    def __init__(self, nodeID: int, error, time):
        self.erroredNode = nodeID
        self.error = error
        self.errorTime = time
    
    def description(self) -> str:
        return f"unable to connect node {self.erroredNode}. {self.error}"

class Network():
    """ serves a the shared point between all the nodes participating in the simulation,
    allows node to communicat with eachother without needing to implement an API or similar"""

    def __init__(self, networkID: int, delaySeed):
        """ class initializer, it allows to define the networkID and the delays between nodes """
        self.networkID = networkID
        self.delaySeed = delaySeed # Just here so that I don't forget, pensing to be updated
        self.nodeStore = NodeStore()
        self.errorTracker = [] # every time that an error is tracked, add it to the queue
        self.connectionTracker = [] # every time that a connection was stablished
        self.connectionCnt = 0
    
    def add_new_node(self, newNode: DHTClient):
        """ add a new node to the DHT network """
        self.nodeStore.add_node(newNode)

    def connect_to_node(self, originNode: int, targetNode: int) -> DHTClient:
        """ get the given DHT client from the PeerStore or raise an error """
        # increase always the total connection counter
        self.connectionCnt += 1
        try:
            node = self.nodeStore.get_node(targetNode)
            self.connectionTracker.append({
                'time': time.time(),
                'from': originNode,
                'to': targetNode,}) 
            return node
        # TODO: at the moment, I only have a peer-missing error, update it to a connection error-rate (usual in Libp2p)
        except NodeNotInStore as e:
            connError = ConnectionError(e.missingNode, e.description, e.time)
            self.errorTracker.append({
                'time': connError.errorTime, 
                'error': connError.description})
            raise connError

    def summary(self):
        """ print the summary of what happened in the network """  
        return {
            'total_nodes': self.nodeStore.len(),
            'attempts': self.connectionCnt,
            'successful': len(self.connectionTracker),
            'failures': len(self.errorTracker)}
    






