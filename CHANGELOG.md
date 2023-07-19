# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Next

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
