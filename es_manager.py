import time, logging, argparse, json, sys
from elasticsearch import Elasticsearch, exceptions

MAX_ATTEMPTS = 10

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger('elasticsearch')

def get_parser(description):
    parser = argparse.ArgumentParser(description=description)

    required_group = parser.add_argument_group("required arguments")
    required_group.add_argument("--bucket", action="store", required=True, help="Bucket name where snapshots are stored")
    required_group.add_argument("--prefix", action="store", required=True, help="Path within S3 bucket for the backups to be stored")

    parser.add_argument("--repository", action="store", default="backup_to", help="Repository name to use in Elasticsearch")
    parser.add_argument("--region", action="store", default="ap-southeast-2", help="S3 bucket region")
    parser.add_argument("--snapshot", action="store", help="Snapshot name to use for the backup/restore (default: all_YYYYMMDDHH)")
    parser.add_argument("--indices", nargs="+", action="store", type=str, help="Backup/restore specific indices (default: all)")
    parser.add_argument("--debug", action="store_true", default=False, help="print debug information")
    parser.add_argument("--eshost", action="store", default="localhost", help="Elasticsearch host")
    parser.add_argument("--esport", action="store", default=9200, help="Elasticsearch port")
    parser.add_argument("--esproto", action="store", default="http", help="Protocol to use when talking to ES (default: http)")
    parser.add_argument("--esauthcfg", action="store", default="/etc/default/elasticsearch-snapshots", help="Configuration file that contains credentials to auth against ES")

    return parser

class ElasticsearchSnapshotManager:
    def __init__(self, options):
        console = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console.setFormatter(formatter)
        logger.addHandler(console)

        self.repository = options.repository
        self.bucket = options.bucket
        self.region = options.region
        self.prefix = options.prefix
        self.snapshot = options.snapshot
        self.host = options.eshost
        self.port = options.esport
        self.protocol = options.esproto
        self.authcfg = options.esauthcfg

        try:
            from configobj import ConfigObj
            authcfg = ConfigObj(self.authcfg)

            self.username = authcfg['USERNAME']
            self.password = authcfg['PASSWORD']
        except (KeyError, ImportError):
            self.username = None
            self.password = None

        self.connect()
        self.s3_repo()

    def connect(self):
        counter = 0
        self.success = False

        if self.username and self.password:
            url = "%s://%s:%s@%s:%d" % (self.protocol, self.username, self.password, self.host, self.port)
        else:
            url = "%s://%s:%d" % (self.protocol, self.host, self.port)

        while True:
            try:
                self.es = Elasticsearch([url])
                self.es.cluster.health(wait_for_status='green', request_timeout=20)
                self.success = True
                break
            except exceptions.ConnectionError as e:
                logger.warning('Still trying to connect to Elasticsearch...')
                counter += 1

                if counter == MAX_ATTEMPTS:
                    break

            logger.info('Sleeping 10 seconds...')
            time.sleep(10)

    def s3_repo(self):
        if self.success:
            # Get snapshot client handler
            self.sh = self.es.snapshot

            conn = self.es.transport.get_connection()

            logger.info('Creating/Updating repository %s' % self.repository)

            repo_settings = {
                "type": "s3",
                "settings": {
                    "bucket":                       self.bucket,
                    "region":                       self.region,
                    "base_path":                    '/' + self.prefix,
                    "max_restore_bytes_per_sec":    '200mb',
                    "max_snapshot_bytes_per_sec":   '200mb'
                }
            }

            # Make the request to create/update the repo. Can't use create_repository() as it tries to create the S3 bucket itself
            conn.perform_request('PUT', '/_snapshot/%s' % self.repository, body=json.dumps(repo_settings), timeout=300)
