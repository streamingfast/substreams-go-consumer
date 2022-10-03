## Substreams Consumer

A small program that can be used to "bootstrap" a Substreams remote files on a particular endpoint. We use that tool internally to "backprocess" and keep live a given Substreams.

The tool while it runs exposes a bunch of metrics (`0.0.0.0:9102` by default) about the consumption process as well as tracking the cursor and some key metrics of a particular Substreams run.

It can be seen loosely as a Substreams manager.
