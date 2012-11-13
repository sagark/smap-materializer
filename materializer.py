from twisted.internet import reactor, protocol, task, interfaces, threads
from twisted.web.client import getPage
from twisted.python import log
from twisted.enterprise import adbapi
from smap.drivers.expr import ExprDriver
from smap.archiver.stream import *
from zope.interface import implements
from get_republish import RepublishListener
from smap.archiver.queryparse import parse_opex
from smap.archiver.data import SmapData
from smap.core import Timeseries
import threading
import json
import sys
import psycopg2
import readingdb
import shelve
import signal
readingdb.db_setup('localhost', 4242)
#maybe give this an api so that users can request materialization from a 
# the server without 
#having to run their own smap?
from twisted.protocols import basic


## Need a way to store state in case Materializer dies - we don't want to 
## have to recompute everything on a restart, although that's what currently
## happens

LOCAL_URL = "http://localhost:8079/api/query?"
LOCAL_QUERYSTR = "select * where not has Metadata/Extra/Operator"
REAL_URL = "http://new.openbms.org/backend/api/query?"
REAL_QUERYSTR = "select * where uuid like 'a2%'"
URL_TO_USE = ""
QUERYSTR_TO_USE = ""


USE_LOCAL = True
if USE_LOCAL:
    URL_TO_USE = LOCAL_URL
    QUERYSTR_TO_USE = LOCAL_QUERYSTR
else:
    URL_TO_USE = REAL_URL 
    QUERYSTR_TO_USE = REAL_QUERYSTR

class Materializer:
    def __init__(self, existing_streams={}):
        """ Initialize our list of existing streams, we eventually want to
        allow this to be loaded from file. Additionally, here we load 
        information about which operators should be applied to which drivers
        including an 'all' field."""

        self.STREAMS_TO_OPS = {'all': None}
        self.EXISTING_STREAMS = existing_streams
        self.republisher = None
        self.persist = StreamShelf()
        self.EXISTING_STREAMS = self.persist.read_shelf() #fill existing streams

        db = adbapi.ConnectionPool('psycopg2', host='localhost', database='archiver', user='archiver', password='password')


        self.data_proc = SmapData(db)
        #with stored data
        #need to run through these at the start and process any unprocessed data


    def fetchExistingStreams(self):
        d = getPage(URL_TO_USE, method='POST', postdata=QUERYSTR_TO_USE)
        d.addCallback(self.check_and_add)
        # here, need to add polling republish

    def check_and_add(self, stream_list):
        """ Compares against [data structure here] of streams to detect new streams,
        starts desired operators on new streams."""
        streams = json.loads(stream_list)
        #print(streams)
        newstreams = 0
        for stream in streams:
            if stream['uuid'] not in self.EXISTING_STREAMS:
                # check if the uuid is in EXISTING_STREAMS. If it isn't add it
                # and then use its tags to determine if we need to start applying
                # ops
                self.EXISTING_STREAMS[stream['uuid']] = StreamWrapper(stream['uuid'], stream)
                print("added and initializing processing" + str(stream))
                for op in self.EXISTING_STREAMS[stream['uuid']].ops:
                    self.process(self.EXISTING_STREAMS[stream['uuid']], op)
                newstreams += 1
            else:
                #print("stream already has processing tasks")
                pass
        print("found " + str(len(streams)) + " streams")
        print("added " + str(newstreams) + " new streams")
        self.republisher.update_streamlist(self.EXISTING_STREAMS)
        #print(self.republisher.streamlist)
        self.persist.write_shelf(self.EXISTING_STREAMS)

    def process(self, stream_wrapped, op='subsample(300)'):
        """ Start processing historical data"""
#        print(stream_wrapped.metadata)
        op_a = parse_opex(op)#get_operator(op, op_args)
   
#        print(op)
  
        d_spec = {'start': 100000, 'end': 1000000000000000000, 'limit': [10000, 10000], 'method': 'data'}
        cons = PrintConsumer(stream_wrapped)
        cons.materializer = self
        op_app = OperatorApplicator(op_a, d_spec, cons)
        op_app.DATA_DAYS = 100000000000
        streamid = fetch_streamid(stream_wrapped.uuid)
        op_app.start_processing(((True, [stream_wrapped.metadata]), (True, [[stream_wrapped.uuid, streamid]])))


        print("\n\n\n")            

class StreamWrapper(object):
    """ Represents a stream, must hold uuid, other metadata, and unprocessed 
    live data"""
    def __init__(self, uuid, meta):
        self.uuid = uuid
        self.metadata = meta
        self.received = []
        self.latest_processed = 0
        self.ops = ['subsample(300)'] # every stream has at least subsample300
    
    def new_live_pt(self, pt):
        """ Add a point to the list of unprocessed data."""
        self.received.append(pt)
        print("added: " + str(pt))


    @property
    def recent_hist(self):
        """ Return data obtained from republish, which we will continue to 
        process as if it were historical."""
        if (len(self.received) == 0):
            return None # if there is no recent hist, return None
        self.latest_processed = self.received[-1][0]
        out = self.received
        self.received = []
        return out

    def __str__(self):
        return uuid

    def __eq__(self, other):
        return str(self) == str(other)


class PrintConsumer(object):
    implements(interfaces.IFinishableConsumer)

    def __init__(self, stream_wrapped):
        self.stream_wrapped = stream_wrapped
        self.materializer = None
        self.data = ""

    def registerProducer(self, producer, streaming):
        pass
    
    def unregisterProducer(self):
        pass

    def write(self, data):
        #data = json.loads(data)
        self.data += data
        print(self.data)
        # now here, we want to store the data back to db and update the
        #stream_wrapped's latest_processed num
        # then check for more points
    #    self.materializer.data_proc.add("95la69dh2VbG38357gTpjAdfdCXqOoivW2RW", data)

    
    def finish(self):
        # here, we want to check to see if there is any built up data
        #
        self.stream_wrapped.d = self.data
        data = json.loads(self.data)
        data = dict((('/' + v['uuid'], v) for v in data))
        print(data)
        self.materializer.data_proc.add(2, data)


class StreamShelf(object):
    """Manages the shelf that stores stream op data"""
    def __init__(self):
        self.shelf_file = 'stream_shelf'
        
    def read_shelf(self):
        s = shelve.open(self.shelf_file)
        # now build the existing streams dict
        exist = {}
        for key in s.keys():
            exist[str(key)] = s[str(key)]
        s.close()
        return exist

    def write_shelf(self, existing):
        s = shelve.open(self.shelf_file)
        for key in s.keys():
            del s[str(key)]
        for key in existing:
            s[str(key)] = existing[str(key)]
        s.close()

def fetch_streamid(uuid):
    conn = psycopg2.connect(database="archiver", host="localhost", user="archiver", password="password")
    cur = conn.cursor()
    cur.execute("SELECT * FROM stream where uuid = '" + uuid + "';")
    return cur.fetchone()[0]

if __name__ == '__main__':
    db = readingdb.db_open('localhost')
    log.startLogging(sys.stdout) # start the twisted logger, takes everything from stdout too
    m = Materializer()
    a = task.LoopingCall(m.fetchExistingStreams)
    a.start(5)
    republisher = RepublishListener()
    m.republisher = republisher

    threads.deferToThread(republisher.start)

    reactor.run()
