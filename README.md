# TCPCALL network transport

## Summary

In general, ``tcpcall`` is a packet oriented network
transport based on TCP/IP.

It aims to simplify communication between services using
 commonly used patterns like:

* Request-Reply;
* Cast;
* Balancing requests between servers;
* Sharding requests between servers.

Main features are:

* packet oriented;
* reuse TCP connection for multiple simultaneous requests;
* connection pools;
* reconfigure on the fly;
* automatic handle of disconnects;
* upstream communication, i.e. when server send some data
 to connected clients;
* flow control;
* no extra software dependencies - libraries use only standard
 libraries.

``tcpcall`` usage is limited to local, protected networks -- there
are no authentication and/or traffic ciphering. It was developed
as an interconnect between services.

For now two libraries are implemented: Erlang and Golang. They
can communicate together, providing efficient and convenient
way to integrate services written on these programming languages.

## API Documentation

* For Erlang lib - [erlang/README.md](erlang/README.md);
* For Golang lib - [golang/README.md](golang/README.md).

## License

Uses FreeBSD 2-clause license. Full text of the License available
in ``LICENSE`` file.
