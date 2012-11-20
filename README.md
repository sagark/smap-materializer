smap-materializer
=================


Currently works:
* "Stream" focused operators (e.g. running subsample for every stream, lots of operators for one stream)
* New streams detected as they appear, subsample(300) and subsample(3600) is run for all of them
* Subsample(300) and subsample(3600) are updated every 5 mins (or one hour) for each stream
* Setting proper metadata so that powerdb recognizes these as subsample streams (i.e. this is now capable of being actually deployed at least for auto-subsampling, barring any stability issues)
* Graceful restart after failure (don't have to recompute everything, just pickup from where we left off)
* "Query" focused operators (e.g. queries used by berkeley.openbms, lots of streams for one set of operators)

Needs to be implemented:
* Load from DB instead of shelves
* What happens if a stream that we're computing for goes down?
* Republisher (I'm not really sure if this needs to be used... seems simpler just to keep pulling directly from readingdb and keep track of latest time + polling)
* Propagating metadata

How to use:
* Adding a stream
    * Start your driver
    * Materializer will auto-detect the stream and start subsample(300) and subsample(3600) for it (including all historical data) with no need for user action
    * If you need to add custom ops, stop materializer, edit the shelf with python and restart materializer (this is annoying, will be fixed once DB is integrated)
