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

    print "Running reconfigure..."
    res = r.table("foo").reconfigure(num_servers, num_servers).run(conn)
    print res

    print "OK, new config is:"
    pprint.pprint(r.table_config("foo").run(conn))

    cluster.check_and_stop()
print "Done."

