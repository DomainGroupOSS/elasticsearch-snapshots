import time, logging, argparse, json, sys
from elasticsearch import Elasticsearch, exceptions

MAX_ATTEMPTS = 10

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger('elasticsearch')

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

        self.connect()
        self.s3_repo()

    def connect(self):
        counter = 0
        self.success = False
    
        while True:
            try:
                self.es = Elasticsearch([
                    {'host': self.host, 'port': self.port}
                ])
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
