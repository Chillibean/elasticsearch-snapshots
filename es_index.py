#!/usr/bin/env python

import time, logging, argparse, json, sys, socket, datetime
from elasticsearch import Elasticsearch, exceptions

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('elasticsearch')

class ElasticsearchIndexManager:
    def __init__(self, options):
        syslog = logging.SysLogHandler(address='/dev/log')
        logger.addHandler(syslog)

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
                self.es.cluster.health(wait_for_status='yellow', request_timeout=20)
                self.success = True
                break
            except exceptions.ConnectionError as e:
                logger.warning('Still trying to connect to Elasticsearch...')
                counter += 1

                if counter == MAX_ATTEMPTS:
                    break

            logger.info('Sleeping 10 seconds...')
            time.sleep(10)

            
def trim_indices(options):
    esm = ElasticsearchIndexManager(options)
    indices = esm.es.indices.get(index=options.index+"*",expand_wildcards="all")

    days = datetime.timedelta(int(options.indexage))
    for index in indices:
        age = datetime.datetime.now() - datetime.datetime.fromtimestamp(float(indices[index]['settings']['index']['creation_date'])/1000.0)
        if age > days:
            logger.info( index + " " + str(datetime.datetime.fromtimestamp(float(indices[index]['settings']['index']['creation_date'])/1000.0)) + " " + str(age))
            esm.es.indices.close(index=index)
            logger.info( index + " closed")
            esm.es.indices.delete(index=index)
            logger.info( index + " deleted")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="this script will trim indices older than a number of days")
    parser.add_argument("--debug", action="store_true", default=False, help="print debug information")
    parser.add_argument("--eshost", action="store", default="localhost", help="Elasticsearch host")
    parser.add_argument("--esport", action="store", default=9200, help="Elasticsearch port")
    parser.add_argument("--esproto", action="store", default="http", help="Protocol to use when talking to ES (default: http)")
    parser.add_argument("--esauthcfg", action="store", default="/etc/default/elasticsearch-snapshots", help="Configuration file that contains credentials to auth against ES")
    parser.add_argument("--master", action="store_true", help="Only run if we're on the Master")
    parser.add_argument("--wait", action="store_true", default=True, help="Wait for the backup to complete")
    parser.add_argument("--indexage", action="store", default=14, help="Oldest index to keep live")
    parser.add_argument("--index", action="store", help="Index to check")
    
    options = parser.parse_args()

    if options.debug:
        logger.setLevel(logging.DEBUG)

    trim_indices(options)
    
