#!/usr/bin/env python

import time, logging, argparse, json, sys, socket
from es_manager import ElasticsearchSnapshotManager, get_parser
from elasticsearch import exceptions

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('elasticsearch')

def take_snapshot(options):
    esm = ElasticsearchSnapshotManager(options)

    if options.master:
        masterip = esm.es.cat.master()
        mip = masterip.split()[2]
        myip = [l for l in ([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1], [[(s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) if l][0][0]

        if myip != mip:
            logger.info("Not running because we're not the master")
            sys.exit()

    logger.info("Running because we're the master")
        
    
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
    parser = get_parser("This script will take a snapshot and upload to S3")
    parser.add_argument("--master", action="store_true", help="Only run if we're on the Master")
    parser.add_argument("--wait", action="store_true", default=True, help="Wait for the backup to complete")
    parser.add_argument("--keep", action="store", default=60, help="Number of Elasticsearch snapshots to keep in S3")

    options = parser.parse_args()

    if options.debug:
        logger.setLevel(logging.DEBUG)

    take_snapshot(options)
