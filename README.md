#### Preliminary Starting Steps

Prepare third-party dependencies.

```
$ git clone https://github.com/catid/wirehair wirehair/wirehair
```

Run meeting point service.

```
$ RUST_LOG=info \
    cargo run --release -- <HOST_NAME> --plaza-service <N>
```

Replace `<HOST_NAME>` with server's publicly-known host name, e.g. `localhost`.
Replace `<N>` with the number of peers to join the network.

Then run each peer in a dedicated shell.

```
$ RUST_LOG=info \
    cargo run --release -- <HOST_NAME> --plaza <MEETING_POINT_HOST_NAME>
```

Peer prints `READY` after joining the network and finishing initialization.

To shut down peers and meeting point service, send `SIGINT` to them.
Notice that peers attempt to send `LEAVE` message to meeting point upon shutting
down, so it's better to shut down meeting point service only after all peers are
gone, i.e.

```
$ pkill -f "entropy.*--plaza "
$ pkill -f "entrypy"
```
