#!/usr/bin/env python
# Copyright 2010-2014 RethinkDB, all rights reserved.
import sys, os, pprint, time, traceback
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir, 'common')))
import driver, scenario_common, utils
from vcoptparse import *
r = utils.import_python_driver()

op = OptParser()
scenario_common.prepare_option_parser_mode_flags(op)
opts = op.parse(sys.argv)

with driver.Metacluster() as metacluster:
    cluster = driver.Cluster(metacluster)
    executable_path, command_prefix, serve_options = scenario_common.parse_mode_flags(opts)
    num_servers = 5
    print "Spinning up %d processes..." % num_servers
    files = [driver.Files(metacluster,
                          log_path = "create-output-%d" % (i+1),
                          machine_name = "server_%d" % (i+1),
                          executable_path = executable_path,
                          command_prefix = command_prefix)
        for i in xrange(num_servers)]
    procs = [driver.Process(cluster,
                            files[i],
                            log_path = "serve-output-%d" % (i+1),
                            executable_path = executable_path,
                            command_prefix = command_prefix,
                            extra_options = serve_options)
        for i in xrange(num_servers)]
    for p in procs:
        p.wait_until_started_up()
    cluster.check()

    print "Creating a table..."
    conn = r.connect("localhost", procs[0].driver_port)
    r.db_create("test").run(conn)
    r.table_create("foo").run(conn)

    # Insert some data so distribution queries can work
    r.table("foo").insert([{"x":x} for x in xrange(100)]).run(conn)

    for num_shards in xrange(1, num_servers+1):
        for num_replicas in xrange(1, num_servers+1):
            print "Configuring for %d shards, %d replicas" % (num_shards, num_replicas)
            conf = r.table("foo").reconfigure(num_shards, num_replicas).run(conn)
            print "OK, new config is:"
            pprint.pprint(conf)
            assert len(set(s["directors"][0] for s in conf["shards"])) == num_shards, \
                "table.reconfigure() should give distinct directors"
            # Wait for the reconfigure to finish (or else the next one might fail because
            # it tries to run a distribution query)
            for i in xrange(10):
                # sleep before the first attempt, to avoid a situation where our read
                # succeeds before the reconfigure has even begun and so we think the
                # reconfigure is finished.
                time.sleep(3)
                try:
                    r.table("foo").run(conn)
                except r.RqlRuntimeError, e:
                    pass
                else:
                    break
            else:
                raise RuntimeError("table took too long to reshard")

    cluster.check_and_stop()
print "Done."

