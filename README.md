# Go Chord

This package provides a Golang implementation of the Chord protocol.
Chord is used to organize nodes along a ring in a consistent way. It can be
used to distribute work, build a key/value store, or serve as the underlying
organization for a ring overlay topology.

The protocol is seperated from the implementation of an underlying network
transport or RPC mechanism. Instead Chord relies on a transport implementation.
A TCPTransport is provided that can be used as a reliable Chord RPC mechanism.

# Documentation

To view the online documentation, go [here](http://godoc.org/github.com/armon/go-chord).

