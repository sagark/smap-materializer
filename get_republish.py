import pycurl, json
from twisted.internet import reactor
import sys

STREAM_URL = 'http://localhost:8079/republish'

class RepublishListener:
    def __init__(self):
        self.buffer = ""
        self.conn = pycurl.Curl()
        self.conn.setopt(pycurl.URL, STREAM_URL)
        self.conn.setopt(pycurl.WRITEFUNCTION, self.on_receive)
#        self.conn.setopt(pycurl.VERBOSE, 1)
        self.streamlist = None

    def start(self):
        self.conn.perform()


    def on_receive(self, data):
        self.buffer += data
        if data.endswith("") and self.buffer.strip():
            stream_reading = json.loads(self.buffer)
            print(stream_reading)
            if not reactor.running:
                sys.exit(0)
            self.buffer = ""
            if self.streamlist:
                print(self.streamlist)
                print("printed streamlist")
            else:
                print("streamlist not updated")
            if stream_reading[stream_reading.keys()[0]]['uuid'] in self.streamlist:
                for reading in stream_reading[stream_reading.keys()[0]]['Readings']:
                    self.streamlist[stream_reading[stream_reading.keys()[0]]['uuid']].new_live_pt(reading)

    def update_streamlist(self, streamlist):
        self.streamlist = streamlist


if __name__ == '__main__':
    repub = RepublishListener()
    repub.start()
