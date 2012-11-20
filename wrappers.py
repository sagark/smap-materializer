"""Wrappers for use by the materializer"""




class StreamWrapper(object):
    """ Represents a stream, must hold uuid, other metadata, and unprocessed 
    live data"""
    def __init__(self, uuid, meta):
        self.uuid = uuid
        self.metadata = meta
        self.received = []
        self.latest_processed = 0
        self.ops = ['subsample(300)', 'subsample(3600)'] # every stream has at least subsample300
    
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


class MultiQueryWrapper(object):
    """ Represents a query that requires multiple streams for processing """
    def __init__(self):
        self.op = "nansum(axis=1) < paste < window(first, field='minute', width=15) < units"
        self.querystr = "select * where (Metadata/Extra/System = 'total' or Metadata/Extra/System = 'electric') and ((Properties/UnitofMeasure = 'kW' or Properties/UnitofMeasure = 'Watts') or Properties/UnitofMeasure = 'W') and not Metadata/Extra/Operator like 'sum%' and not Path like '%demand'"
        self.latest_processed = 0
        self.repeat_time = 15*60

