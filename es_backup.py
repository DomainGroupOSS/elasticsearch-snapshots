#!/usr/bin/env python

import time, logging, argparse, json, sys
from es_manager import ElasticsearchSnapshotManager
from elasticsearch import exceptions

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('elasticsearch')

def take_snapshot(options):
    esm = ElasticsearchSnapshotManager(options)
    sh = esm.sh

    snapshot = options.snapshot and options.snapshot or 'all_' + time.strftime('%Y%m%d%H')

    snapdef = {
        "include_global_state": True
    }

    if options.indices:
        snapdef['indices'] = ','.join(options.indices)
    
    try:
        sh.create(repository=options.repository, snapshot=snapshot, body=json.dumps(snapdef), wait_for_completion=options.wait, request_timeout=7200)

        # Housekeeping - delete old snapshots
        snapshots = sh.get(repository=options.repository, snapshot="_all", request_timeout=120)['snapshots']
        num_snaps = len(snapshots)
        if num_snaps > options.keep:
            up_to = num_snaps - options.keep
            logger.info('TOTAL: %d - Will delete 1 -> %d' % (num_snaps, up_to + 1))
            for snap in snapshots[0:up_to]:
                sh.delete(repository=options.repository, snapshot=snap['snapshot'], request_timeout=3600)
                logger.info('Deleted snapshot %s' % snap['snapshot'])
    except exceptions.TransportError as e:
        pass

if __name__ == '__main__':
    description = """This script will take a snapshot and upload to S3"""
    parser = argparse.ArgumentParser(description=description)
    
    required_group = parser.add_argument_group("required arguments")
    required_group.add_argument("--bucket", action="store", required=True, help="Bucket name where snapshots are stored")
    required_group.add_argument("--prefix", action="store", required=True, help="Path within S3 bucket for the backups to be stored")

    parser.add_argument("--repository", action="store", default="backup_to", help="Repository name to use in Elasticsearch")
    parser.add_argument("--region", action="store", default="ap-southeast-2", help="S3 bucket region")
    parser.add_argument("--snapshot", action="store", help="Snapshot name to use for the backup (default: all_YYYYMMDDHH)")
    parser.add_argument("--indices", nargs="+", action="store", type=str, help="Backup specific indices (default: all)")
    parser.add_argument("--wait", action="store_true", default=True, help="Wait for the backup to complete")
    parser.add_argument("--debug", action="store_true", default=False, help="print debug information")
    parser.add_argument("--eshost", action="store", default="localhost", help="Elasticsearch host")
    parser.add_argument("--esport", action="store", default=9200, help="Elasticsearch port")
    parser.add_argument("--keep", action="store", default=60, help="Number of Elasticsearch snapshots to keep in S3")

    options = parser.parse_args()

    if options.debug:
        logger.setLevel(logging.DEBUG)

    take_snapshot(options)
