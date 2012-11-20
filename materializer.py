#!/usr/bin/python

# twisted/related imports
from zope.interface import implements
from twisted.internet import reactor, protocol, task, interfaces, threads
from twisted.web.client import getPage
from twisted.python import log
from twisted.enterprise import adbapi

# smap imports
from smap.drivers.expr import ExprDriver
from smap.archiver.stream import *
from smap.archiver.queryparse import parse_opex
from smap.archiver.data import SmapData
from smap.core import Timeseries

# materializer specific imports
from wrappers import *
from mat_utils import *

# misc imports
import json
import sys
import readingdb
import shelve


# config
readingdb.db_setup('localhost', 4242)
URL_TO_USE = "http://localhost:8079/api/query?"
QUERYSTR_TO_USE = "select * where not has Metadata/Extra/Operator"

REPUBLISH_LISTEN_ON = False
# end config

class Materializer:
    def __init__(self):
        """ Initialize our list of existing streams, we eventually want to
        allow this to be loaded from file. Additionally, here we load 
        information about which operators should be applied to which drivers
        including an 'all' field."""

        self.republisher = None
        self.stream_persist = StreamShelf('stream')
        self.query_persist = StreamShelf('query')
        self.EXISTING_STREAMS = self.stream_persist.read_shelf() #fill existing streams
        self.EXISTING_QUERIES = self.query_persist.read_shelf()['query'] #[QueryWrapper()] # load with one just for testing


        if self.EXISTING_QUERIES == []:
            # load in default for testing
            self.EXISTING_QUERIES += [QueryWrapper()]

        # setup db
        db = adbapi.ConnectionPool('psycopg2', host='localhost', 
                    database='archiver', user='archiver', password='password')
        self.data_proc = SmapData(db)

        #self.on_start() # restart anything that was processing when the 
                        # materializer shutdown last



    def on_start(self):
        for stream in self.EXISTING_STREAMS:
            stream_list = [self.EXISTING_STREAMS[stream]]
            for op in stream_list[0].ops:
                reactor.callLater(op.refresh_time, self.process, stream_list, 
                                      op, stream_list[0].latest_processed)
                print("restarting " + op.opstr + " for stream " + stream)

        for query in self.EXISTING_QUERIES:
            self.fetchForQuery(query)
            print("(re)staring materialization of " + query.querystr)
            
    def fetchForQuery(self, query):
        d = getPage(URL_TO_USE, method='POST', postdata=query.querystr)
        d.addCallback(self.prepProcessCall,op=query.op)
        

    def prepProcessCall(self, stream_list, op):
        stream_list = json.loads(stream_list)
        streams_wrapped = [StreamWrapper(stream['uuid'], stream) for stream in stream_list]
        self.process(streams_wrapped, op) 


    def fetchExistingStreams(self):
        d = getPage(URL_TO_USE, method='POST', postdata=QUERYSTR_TO_USE)
        d.addCallback(self.periodic_check_and_add)
        # here, need to add polling republish

    def periodic_check_and_add(self, stream_list):
        """ Compares against [data structure here] of streams to detect new streams,
        starts desired operators on new streams."""
        streams = json.loads(stream_list)
        newstreams = 0
        for stream in streams:
            if stream['uuid'] not in self.EXISTING_STREAMS:
                # check if the uuid is in EXISTING_STREAMS. If it isn't add it
                # and then use its tags to determine if we need to start applying
                # ops, start processing historical
                self.EXISTING_STREAMS[stream['uuid']] = StreamWrapper(stream['uuid'], stream)
                print("added and initializing processing" + str(stream))
                for op in self.EXISTING_STREAMS[stream['uuid']].ops:
                    self.process([self.EXISTING_STREAMS[stream['uuid']]], op)
                newstreams += 1

        print("Found " + str(len(streams)) + " total streams")
        print("Added " + str(newstreams) + " new streams")

        if REPUBLISH_LISTEN_ON:
            self.republisher.update_streamlist(self.EXISTING_STREAMS)

        # write back to shelf after each run
        self.stream_persist.write_shelf(self.EXISTING_STREAMS)
        self.query_persist.write_shelf({'query': self.EXISTING_QUERIES})

    def process(self, streams_wrapped, op, start=1):
        """ Apply op to streams in streams_wrapped from start to latest"""
        op_a = parse_opex(op.opstr)

        # this will work at least until the year 33658
        d_spec = {'start': start, 'end': 1000000000000000000, 
                                    'limit': [0, 0], 'method': 'data'}

        cons = ProcessedDataConsumer(streams_wrapped, op)
        cons.materializer = self
        #cons.set_op(op)
        op_app = OperatorApplicator(op_a, d_spec, cons)
        op_app.DATA_DAYS = 100000000000
        #streamid = fetch_streamid(stream_wrapped.uuid)
        print(op)
        metas = [getattr(stream, 'metadata') for stream in streams_wrapped]
        ids = [[getattr(stream, 'uuid'), fetch_streamid(getattr(stream,'uuid'))] for stream in streams_wrapped]
        op_app.start_processing(((True, metas), (True, ids)))


class ProcessedDataConsumer(object):
    implements(interfaces.IFinishableConsumer)

    def __init__(self, streams_wrapped, op):
        #hacky fix, change later
        self.streams_wrapped = streams_wrapped
        self.materializer = None
        self.data = ""
        self.op = op

    def registerProducer(self, producer, streaming):
        pass
    
    def unregisterProducer(self):
        pass

    def write(self, data):
        """ Store the data as we're receiving it. """
        self.data += data
    
    def finish(self):
        """ Upon completion of data receive, start processing. At the end, 
        check for built up data in self.stream_wrapped.received. If there is
        data, we need to run historical processing on it again, assuming that
        there is a sufficient amount."""
        #self.stream_wrapped.d = self.data
        data = json.loads(self.data)
        print(self.op)
        if len(data[0]["Readings"]) != 0:
            # set metadata as necessary
            data[0]['Metadata']['Extra']['Operator'] = self.op.meta_op
            data[0]['Metadata']['Extra']['SourceStream'] = self.streams_wrapped[0].uuid
            data[0]['Metadata']['Timezone'] = "America/Los_Angeles"
            # set path to reflect that this is a processed version of another stream
            data[0]['Path'] = '/r/' + self.streams_wrapped[0].uuid + '/' + self.op.meta_op

            if 'SourceName' not in data[0]['Metadata'].keys():
                data[0]['Metadata']['SourceName'] = 'awesome'

            # special metadata so that powerdb takes special action for subsamples
            if 'subsample' in self.op.opstr:
                #subsamples have no SourceName so that they don't show up in powerdb
                try:
                    del(data[0]['Metadata']['SourceName'])
                except:
                    pass

            #only update latest processed if something was actually computed
            self.streams_wrapped[0].latest_processed = data[0]["Readings"][-1][0]
            data = dict(((v['Path'], v) for v in data)) 
            self.materializer.data_proc.add(2, data) #store back to db

            if self.op.refresh_time is not None and self.op.refresh_time > 0:
                reactor.callLater(self.op.refresh_time, m.process, self.streams_wrapped, 
                                      self.op, self.streams_wrapped[0].latest_processed)


class StreamShelf(object):
    """Manages the shelf that stores stream op data"""
    def __init__(self, namefront):
        self.shelf_file = namefront+'_shelf'
        
    def read_shelf(self):
        s = shelve.open(self.shelf_file)
        # now build the existing streams dict
        exist = {}
        for key in s.keys():
            exist[str(key)] = s[str(key)]
        s.close()
        if 'query' in self.shelf_file and exist == {}:
            return {'query': []}
        return exist

    def write_shelf(self, existing):
        s = shelve.open(self.shelf_file)
        for key in s.keys():
            del s[str(key)]
        for key in existing:
            s[str(key)] = existing[str(key)]
        s.close()

if __name__ == '__main__':
    # start the twisted logger, takes everything from stdout too
    log.startLogging(sys.stdout) 
    m = Materializer()
    reactor.callLater(1, m.on_start)
    a = task.LoopingCall(m.fetchExistingStreams)
    a.start(5)
    if REPUBLISH_LISTEN_ON:
        republisher = RepublishListener()
        m.republisher = republisher

        threads.deferToThread(republisher.start)

    reactor.run()
