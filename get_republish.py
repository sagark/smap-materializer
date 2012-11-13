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
        self.streamlist = None

    def start(self):
        self.conn.perform()


    def on_receive(self, data):
        self.buffer += data
        if data.endswith("") and self.buffer.strip():
            stream_reading = json.loads(self.buffer)
            self.buffer = ""
            self.append_to_stream(stream_reading)
        return None

    def header(self, buf):
        # Print header data to stderr
        import sys
        sys.stderr.write(buf)
        # Returning None implies that all bytes were written

    def update_streamlist(self, streamlist):
        self.streamlist = streamlist

    def append_to_stream(self, stream_reading):
        """ Here, we want to append to data to the appropriate stream in 
        streamlist """
        print(stream_reading)
        print("reactor:" + str(reactor.running))
        if self.streamlist:
            print(self.streamlist)
            print("printed streamlist")
        else:
            print("streamlist not updated")
        if stream_reading['uuid'] in self.streamlist:
            self.streamlist[stream_reading['uuid']].new_live_pt(stream_reading['Readings'][0])



if __name__ == '__main__':
    repub = RepublishListener()
    repub.start()
