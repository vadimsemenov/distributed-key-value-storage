# Key-Value storage

## TL;DR
Distributed key-value storage based on [RAFT](https://raft.github.io/)
consensus algorithm build on [AKKA](http://akka.io/).

It's coursework for Roman Elizarov's (aka @elizarov) Distributed
Systems course, read in ITMO University, Spring 2016.

## TODO
* complete README
* documentation
* logging
  * handle backups
* client
  * add option to connect to
    * specific node
    * cluster
* consistent hashing
* create independent modules for consensus algorithm and
key value storage implementation
* ...

## License

This project is licensed under [WTFPL](http://www.wtfpl.net/)
![WTFPL icon](http://www.wtfpl.net/wp-content/uploads/2012/12/wtfpl-badge-1.png)