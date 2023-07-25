# py-dht
Small and straightforward representation of how a Kademlia-based DHT could be integrated into Ethereum, particularly at [DAS-research](https://github.com/codex-storage/das-research).

## Terminology
All the terms and abbreviations that will be found in the code 
- BPID: Block-Part IDentifier (for both Row and Column IDs)

## Specifications

The code includes a simple logic implementation of a `DHTClient`, which includes:
- [x] basic `Hash` and `BitArray` implementations
- [x] logical `RoutingTable` and `KBucket` that can: 
    - [x] fill its kbuckets with the XX closest peers sharing YY bits with our `NodeID`
    - [x] Give back the closest XX peers to a given Hash
- [x] basic `DHTClient` operations, such as:
    - [x] create a Network interface that can link all the nodes in the network
    - [x] bootstrap and fill up the routing table from ZZ nodes
    - [x] lookup for the closest XX peers to a given Hash
    - [x] Provide a `BPID` to the network
    - [x] Retrieve a `BPID` from the network
- [ ] Make the DHT compatible with random delays and error rates
    - [ ] make randomness and hashes deterministic

## Maintainer
[@cortze](https://github.com/cortze)

## Contributing
Feel free to dive in! Change proposals, issues, and PRs will be more than welcome.

## Support
- The work has been supported by [Codex](https://github.com/codex-storage)
- Feel free to support this project through [Buy Me A Coffee](https://www.buymeacoffee.com/cortze); it would make my day 😊.

## License
MIT, see [LICENSE](./LICENSE) file

