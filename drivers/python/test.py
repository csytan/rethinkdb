import asyncio
import datetime

import rethinkdb as r


query = r.db('nohuck').table('videos').limit(10)
query = (r.db('nohuck')
    .table('videos')
    .map(lambda video: video['tags'])
    .reduce(lambda left, right: left.add(right)))

def test_sync():
    import sys
    # use system rethinkdb driver
    sys.path.pop(0)
    for i in range(100):
        r.connect('127.0.0.1', 28015).repl()
        result = query.run()
    

def simple_timer(cb, loops=1):
    print(cb)
    while loops:
        loops -= 1
        start = datetime.datetime.now()
        cb()
        end = datetime.datetime.now()
        seconds = (end - start).seconds
        print(seconds)
        

def test_async():
    futures = []
    @asyncio.coroutine
    def run():
        for i in range(100):
            futures.append(query.run_async())
        for f in futures:
            yield from f
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
    #print(futures)


simple_timer(test_sync, 4)


