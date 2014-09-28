import datetime
import rethinkdb as r


def test_async():
    futures = []
    for i in range(500):
        futures.append(
            r.db('nohuck').table('videos').limit(10).run_async())
    
    while len(futures):
        finished = [f for f in futures if f.done()]
        futures = [f for f in futures if not f.done()]
        for f in finished:
            [item for item in f.result()]
            print('finished')
        print(len(futures))
    
def test_sync():
    for i in range(500):
        r.connect('localhost', 28015).repl()
        result = r.db('nohuck').table('videos').limit(10).run()
        [item for item in result]
    print('done')
    

def simple_timer(cb, loops=1):
    print(cb)
    while loops:
        loops -= 1
        start = datetime.datetime.now()
        cb()
        end = datetime.datetime.now()
        seconds = (end - start).seconds
        print(seconds)
        

simple_timer(test_sync, 3)