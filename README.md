# Go Chord
This package provides a Golang implementation of the Chord protocol.
Chord is used to organize nodes along a ring in a consistent way. It can be
used to distribute work, build a key/value store, or serve as the underlying
organization for a ring overlay topology.

The protocol is seperated from the implementation of an underlying network
transport or RPC mechanism. Instead Chord relies on a transport implementation.
The following transport implementations are provided:

- TCPTransport
- GRPCTransport

#### TCPTransport
The TCPTransport is implemented using the native network stack and gob encoding.

#### GRPCTransport
The GRPCTransport uses grpc to perform rpc operations.

# Development
When using grpc and changes need to be made, add the appropriate code to the net.proto file, 
and re-generate the code using:

	make protoc

# Documentation
To view the online documentation, go [here](http://godoc.org/github.com/euforia/go-chord).

