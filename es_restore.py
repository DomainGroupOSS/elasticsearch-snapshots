#!/usr/bin/env python

import time, logging, argparse, json, sys, httplib, urllib2, facter
from es_manager import ElasticsearchSnapshotManager, get_parser
from elasticsearch import exceptions

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('elasticsearch')

def restore_snapshot(options):
    esm = ElasticsearchSnapshotManager(options)
    sh = esm.sh

    if options.list:
        snapshots = sh.get(repository=options.repository, snapshot="_all", request_timeout=120)['snapshots']
        print 'Total number of snapshots: %d\n' % len(snapshots)

        for i, snap in enumerate(snapshots):
            print '[%4d] NAME: %14s | START/END: %s -> %s | STATE: %7s | INDICES: %s' % (i + 1, snap['snapshot'], snap['start_time'], 'end_time' in snap and snap['end_time'] or '...', snap['state'], ', '.join(snap['indices']))
    else:
        fail = True

        snapdef = {
            "include_global_state": True
        }

        if options.indices:
            snapdef['indices'] = ','.join(options.indices)

        if options.snapshot:
            try:
                snapshot = sh.get(repository=options.repository, snapshot=options.snapshot)['snapshots'][0]
            except:
                logger.error('Snapshot "%s" not found in S3 bucket "%s" for repository "%s"' % (options.snapshot, options.bucket, options.repository))
                snapshot = None
        else:
            try:
                snapshot = sh.get(repository=options.repository, snapshot="_all", request_timeout=120)['snapshots'][-1]
            except:
                snapshot = None

        if snapshot:
            try:
                # Perform the restore
                logger.info('Starting restore of snapshot "%s" from bucket "%s"' % (snapshot['snapshot'], options.bucket))

                try:
                    sh.restore(repository=options.repository, snapshot=snapshot['snapshot'], body=json.dumps(snapdef),
                        wait_for_completion=options.wait, request_timeout=7200)
                    logger.info('Restore of snapshot "%s" has started' % snapshot['snapshot'])

                    fail = False

                    if options.slackurl:
                        post_to_slack(url=options.slackurl, snapshot=snapshot['snapshot'], prefix=options.prefix, channel=options.slackchan)
                except exceptions.TransportError as e:
                    logger.warning('Unable to restore snapshot "%s": %s' % (snapshot['snapshot'], e.error))
            except NameError as e:
                logger.warning('No snapshots found for bucket "%s" with prefix "%s"' % (options.bucket, options.prefix))
        else:
            logger.error('Unable to fetch a snapshot to restore from')

        sys.exit(fail)

def post_to_slack(url, channel, snapshot, prefix):
    f = facter.Facter()
    msg = 'Started Elasticsearch restore of snapshot "%s" (from "%s") into stack "%s"' % (snapshot, prefix, f.lookup('cloudformation_stack_name'))

    data = json.dumps({
        'channel': channel,
        'username': f.lookup('cloudformation_stack_name').replace('-', ' '),
        'attachments': [
            {
                'fallback': msg,
                'pretext': msg,
                'color': 'good',
                'fields': [
                    {
                        'title': f.lookup('hostname'),
                        'value': 'You will not be able to connect to Elasticsearch for a little while while the restore is happening.'
                    }
                ]
            }
        ]
    })

    req = urllib2.Request(url, data, {"Content-type": "application/json"})
    reqh = urllib2.urlopen(req)
    reqh.close()

def post_to_flowdock(token, snapshot, prefix):
    try:
        import requests
    except:
        print 'Missing requests library needed by flowdock ... silenty ignoring'
        return

    f = facter.Facter()
    username = f.lookup('cfn_stack_name').replace('-', ' '),

    msg = 'Started Elasticsearch restore of snapshot "%s" (from "%s") into stack "%s"\n' % (snapshot, prefix, f.lookup('cfn_stack_name'))
    msg = msg + 'You will not be able to connect to Elasticsearch for a little while while the restore is happening.'

    try:
        r = requests.post("https://api.flowdock.com/messages/chat/" + token, data={'content': msg, 'event': 'comment', 'external_user_name': username})
        if r.status_code != 200:
            print 'Problem while posting to flowdock. Verify the token'
    except e:
            print 'error while posting to flowdock ' + e.msg

if __name__ == '__main__':
    parser = get_parser("This script will restore a snapshot into ES running on localhost:9200 (by default)")
    parser.add_argument("--list", action="store_true", help="List snapshots ONLY")
    parser.add_argument("--slackurl", action="store", help="Slack URL to post to on success")
    parser.add_argument("--flowdock", action="store", help="Flowdock Token to post on success")
    parser.add_argument("--slackchan", action="store", default="#linux", help="Slack channel to post to on success")
    parser.add_argument("--wait", action="store_true", default=False, help="Wait until backup completes")

    options = parser.parse_args()

    if options.debug:
        logger.setLevel(logging.DEBUG)

    restore_snapshot(options)
