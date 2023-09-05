#### Preliminary Starting Steps

Prepare third-party dependencies.

```
$ git clone https://github.com/catid/wirehair wirehair/wirehair
```

Run the plaza service
```
$ RUST_LOG=info \
    cargo run --release -- <HOST_NAME> --plaza-service <N>
```

Replace `<HOST_NAME>` with server's publicly-known host name, e.g. `localhost`.
Replace `<N>` with the number of peers to join the network.

Then run each peer in a dedicated shell
```
$ RUST_LOG=info \
    cargo run --release -- <HOST_NAME> --plaza <PLAZA_HOST_NAME>
```

Peer prints `READY` after joining the network and finishing initialization.

Get a list of all peer URI's
```
$ curl http://<PLAZE_HOST_NAME>:8080/run | jq -r .Ready.participants[].Peer.uri
```

Perform a PUT benchmark on a peer
```
$ curl -X POST http://<PEER_URI>/benchmark/put
```

Poll benchmark status
```
$ curl http://<PEER_URI>/benchmark/put | jq
```

Shutdown peers
```
$ curl -X POST http://<PLAZE_HOST_NAME>:8080/shutdown
```

Shutdown plaza service by sending a `SIGTERM`
```
$ pkill -TERM entropy
```
