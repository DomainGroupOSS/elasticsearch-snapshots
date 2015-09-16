# What's in here?

This repo contains a set of scripts used to manage Elasticsearch snapshots in S3.

# Usage

I place all 3 scripts in a directory (such as `/usr/local/bin`) and you then run either `es_backup.py` or `es_restore.py` (see `--help`). Example usage:

## Backup

Take a snapshot and upload into the named bucket using said prefix, and wait until it is complete before returning:

```
/usr/local/bin/es_backup.py --bucket elasticsearch-snapshots-bucket --prefix Snapshot_Prefix --wait
```

## Restore

Restore the latest snapshot, but only the named index:

```
/usr/local/bin/es_restore.py --bucket elasticsearch-snapshots-bucket --prefix Snapshot_Prefix --indices cats
```

# WARNING

These scripts were written to meet an immediate need and they actually get pushed to our Elasticsearch instances by our config management system.
They were NOT written in generic fashion for various different environments in mind, so you may need to do some tweaking.

Ideally this should be a pip package that you can just install, specially because there are dependencies such as facter, elasticsearch, etc.

Unfortunately, no time to improve these scripts, but if they helped you out and you want to contribute back, pull requests are more than welcome!!

