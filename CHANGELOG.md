# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v1.1.4

* This is a re-release of `v1.1.3` which failed to build.

## v1.1.3

* Fixed spurious error reporting when the sinker is terminating or has been canceled.

* Updated `substreams` dependency to latest version `v1.3.7`.

## v1.1.2

* Improved `substreams stream stats` log line but now using `substreams_sink_progress_message_total_processed_blocks` for `progress_block_rate` replacing the `progress_msg_rate` which wasn't meaningful anymore (and broken because the metric was never updated).

* Fixed a crash when providing a single block argument for block range if it's the same as the Substreams' start block.

* Added `--network` flag and handling proper handling.

## v1.1.1

### Substreams Progress Messages

> [!IMPORTANT]
> This client only support progress messages sent from a to a server with substreams version >=v1.1.12

* Bumped substreams-sink to `v0.3.1` and substreams to `v1.1.12` to support the new progress message format. Progression now relates to **stages** instead of modules. You can get stage information using the `substreams info` command starting at version `v1.1.12`.

#### Changed Prometheus Metrics

* `substreams_sink_progress_message` removed in favor of `substreams_sink_progress_message_total_processed_blocks`
* `substreams_sink_progress_message_last_end_block` removed in favor of `substreams_sink_progress_message_last_block` (per stage)

#### Added Prometheus Metrics

* added `substreams_sink_progress_message_last_contiguous_block` (per stage)
* added `substreams_sink_progress_message_running_jobs`(per stage)

### Changed

* **Breaking** Flag shorthand `-p` for `--plaintext` has been re-assigned to Substreams params definition, to align with `substreams run/gui` on that aspect. There is no shorthand anymore for `--plaintext`.

  If you were using before `-p`, please convert to `--plaintext`.

  > **Note** We expect that this is affecting very few users as `--plaintext` is usually used only on developers machine.

### Added

* Added support for `--params, -p` (can be repeated multiple times) on the form `-p <module>=<value>`.

## v1.1.0

### Changed

* Added possibility to pass extra headers when doing the request.

* Head tracker is now working on any Substreams supported network.

* Removed the need to have a Firehose instance to track head.

## v1.0.0

* Initial commit
