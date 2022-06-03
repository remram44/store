# What is this?

This is a toy distributed storage system, written in Rust. It is currently named "store" though this will obviously have to change, suggestions welcome.

You can use this to build a pool of storage from many hard drives, and allow clients to read and write to them.

The design is heavily inspired by [Ceph](https://ceph.io/) and [SeaweedFS](https://github.com/chrislusf/seaweedfs).

# Current status

There is currently no coordination between storage daemons, but you can store data on a single storage daemon (= single hard drive). I am working on the coordination.

# Architecture

## Master

The master server coordinates the whole system. It knows about the storage daemons, the storage maps for each pool that directs objects to specific storage daemons, and the authorizations of specific clients.

Storage daemons connect to the master over TCP/mTLS to get the secret server key and the storage map for each pool they participate in. They register themselves on first connection.

Clients connect to the master over TCP/TLS to get a client key that they can use to talk to storage daemons, and to get the storage map for the pool they want to use.

It would make sense to have multiple masters, and have them pick a leader via a consensus protocol. This would provide high availability, preventing the whole cluster from going down if the master goes down.

Example usage:

```
target/release/store master \
    --peer-address 0.0.0.0:4000 \
    --peer-cert tls/master1.crt -peer-key tls/master1.key --peer-ca-cert tls/ca.crt \
    --listen-address 0.0.0.0:4010 \
    --listen-cert tls/master.crt --listen-key tls/master.key
```

### Status

Pretty early, not yet usable. This is not critical for development as I can hardcode the storage map.

## Storage daemons

The storage daemons provide the actual storage. There is one storage daemon per disk; running multiple storage daemons on one machine is fine.

Clients send requests to read and write to the storage daemons over UDP.

Storage daemons connect to each other over TCP/mTLS to exchange data in case of replication or rebalancing (which happens when the storage map changes).

Example usage of storage daemon:

```
target/release/store file-store \
    --peer-address 0.0.0.0:4149 \
    --peer-cert tls/storage001.crt --peer-key tls/storage001.key --peer-ca-cert tls/ca.crt \
    --listen-address 0.0.0.0:4148 \
    --dir /tmp/storage
```

### Status

Serving requests over UDP works.

## Clients

The client reads and writes from a storage pool. A strength of the system is that clients contact storage daemons directly, which improves latency and throughput compared to talking to an intermediary.

To do this they retrieve the storage map from the master, which allows them to compute which storage daemon should hold an object by hashing the object's name.

### Status

We can do read and write requests against a storage daemon directly. Currently working on supporting multiple storage daemons via a storage map. The next step will be getting that storage map from the master.

Example usage of command-line client:

```
target/release/store -v write --storage-daemon 127.0.0.1:4148 --pool testpool testobj --data-literal "hello world"
target/release/store -v write --storage-daemon 127.0.0.1:4148 --pool testpool passwd --data-file /etc/passwd
target/release/store -v read --storage-daemon 127.0.0.1:4148 --pool testpool passwd --offset 20 --length 40
```

## Gateways

Gateways are special clients that act on behalf of others. They adapt our native protocol for use by service that require a different protocol, for example S3, NBD, iSCSI.

### NBD

NBD is the [Network Block Device](https://en.wikipedia.org/wiki/Network_block_device) protocol. It allows exposing a Linux block device over the network. This gateway acts as an NBD server, allowing a Linux machine to use the cluster as a thinly-provisioned block device on top of which a filesystem can be created and mounted (by one client at a time).

This works. It is implemented as an nbdkit plugin.

Example usage:

```
nbdkit target/release/libstore_nbd_gateway.so -f storage_daemon_address=127.0.0.1:4148 pool=testpool image=testblock
mkdir /tmp/storage/testpool
printf '\x06\x40\x00\x00' > /tmp/storage/testpool/74657374626c6f636b # Write metadata in 'testblock': size=100MB
nbd-client localhost 10809 /dev/nbd0
mkfs.ext3 /dev/nbd0
mount /dev/nbd0 /mnt
```

### iSCSI

iSCSI is the most common protocol for accessing block devices over the network.

This needs an implementation of the iSCSI protocol.

### Simple HTTP

A simple HTTP gateway could be developed easily.

### S3

S3 has a lot of surface, not sure I want to implement it. Could an existing gateway be used for this? [Minio](https://min.io/)?

### FUSE

Mounting a filesystem via FUSE requires a separate metadata server to serialize operation so clients have a consistent view of the filesystem.
