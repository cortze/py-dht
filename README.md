# py-dht
Small and simple representation of how a Kademlia based DHT could be integrated into Ethereum and particularly at [DAS-research](https://github.com/codex-storage/das-research).

## Terminology
All the terms and abreviations that will be found in the code 
- BPID: Block-Part IDentifier (for both Row and Column IDs)

## Specifications

The code includes a simple logic implementation of a `DHTClient`, which includes:
- [x] basic `Hash` and `BitArray` implementations
- [x] logical `RoutingTable` and `KBucket` that can: 
    - [x] fill its kbuckets with the XX closest peers sharing YY bits with our `NodeID`
    - [x] Give back the closest XX peers to a given Hash
- [] basic `DHTClient` operations, such as:
    - [] bootstrap and fill up the routing table from ZZ nodes
    - [] lookup for the closest XX peers to a given Hash
    - [] Provide a `BPID` to the network
    - [] Retrieve a `BPID` from the network



