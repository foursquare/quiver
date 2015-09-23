# Quiver Key-Value Server
A light-weight server for querying immutable data stored in HFiles

Quiver provides a low-latency server that can be run directly top of large, generated datasets commonly output by offline/"big data" pipelines -- as an alternative to trying to bulk-load their output back into a mutable databases.

## Protocol
Quiver uses Thrift-RPC-over-HTTP to communicate - standard Thrift RPC calls are encoded and sent as HTTP request/response bodies. This allows any off-the-shelf http tools (eg HAProxy) to interact with this thrift-RPC traffic.

## The HFile Format
HFiles are designed to be written incrementally (metadata is in a "trailer" at the end rather than in a header, so you do not have to buffer the whole dataset while writing) -- and include an index, meaning they can be mapped into memory and used to answer queries quickly "as-is", without needing to build indexes at serving time.

The actual implementation of the HFile file interaction (finding keys, iterating, etc) is implemented in a [standalone library](http://github.com/foursquare/gohfile).
