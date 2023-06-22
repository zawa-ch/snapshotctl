# snapshotctl

Simple backup/snapshot manager

## Overview

A new backup system to replace the backup.sh that was created before.
Made some tweaks and changes, including replacing the management database from JSON to SQLite.

## Dependent package

### Requred

- [bash](https://www.gnu.org/software/bash/)
- [SQLite3](https://www.sqlite.org/index.html)
- [jq](https://jqlang.github.io/jq/)

### Optional

- [Zstandard](https://facebook.github.io/zstd/)
- [gzip](https://www.gnu.org/software/gzip/)
- [xz](https://tukaani.org/xz/format.html)
