## Substreams Sink NoOp

A small program that can be used to "bootstrap" a Substreams remote files on a particular endpoint. We use that tool internally to "backprocess" and given Substreams up to date with the chain.

The tool while it runs exposes a bunch of metrics (`0.0.0.0:9102` by default) about the consumption process as well as tracking the cursor and some key metrics of a particular Substreams run.

## Usage

```bash
substreams-sink-noop mainnet.eth.streamingfast.io:443 https://github.com/streamingfast/substreams-eth-block-meta/releases/download/v0.4.0/substreams-eth-block-meta-v0.4.0.spkg graph_out 0:+200000
```

> **Note** By default, a subsequent run will restart from last saved cursor found by default in file `pwd`/state.yaml, you can pass `--clean` to start from scratch each time.

## Management API

You can use the management API while the application is running to clean the state on next restart.

### Reset state on next restart

```shell
# Reset state on next restart
curl -XPOST -d '{}' localhost:8080/reset_state

# Cancel state reset on next restart
curl -XPOST -d '{}' localhost:8080/cancel_reset_state

# Clean shutdown
curl -XPOST -d '{}' localhost:8080/shutdown
```
