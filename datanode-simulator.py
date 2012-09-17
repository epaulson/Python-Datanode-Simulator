#!/usr/bin/env python
#
# using examples from http://code.activestate.com/recipes/502293-hex-dump-port-forwarding-network-proxy-server/
#
"""
Mockup of a Hadoop HDFS Datanode, using Python and Twisted Matrix

   usage: PROGRAM [options] 

   Options:
   "-h", "--help": This message
   "-v", "--verbose": Increase logging level
   "-l", "--log": Log to file
   "-s", "--storageId": Use specificed string as the base for the storageID string. Will have an integer appended 
   "-n", "--namenode": Connect to specified namenode. Defaults to localhost
   "-c", "--commandport": Connect to that port on the namenode, defaults to 8020
   "-w", "--webport": Built-in webserver should listen on this port, defaults to 8080



Program framework based on example code from Andrew Ellerton
from http://code.activestate.com/recipes/502293-hex-dump-port-forwarding-network-proxy-server/
Copyright (C) 2005-2007 Andrew Ellerton, mail: activestate-at->ellerton.net
"""


import sys, getopt, logging

try: import twisted
except ImportError:
  print "Please install the 'twisted' package from http://twistedmatrix.com/"
  sys.exit(1)

from twisted.internet import reactor
from twisted.internet.protocol import Protocol, Factory, ClientFactory 
from twisted.protocols import basic
from twisted.web import server, resource

from datanode import datanode
from datanode import nodeinfo

if __name__ == "__main__":
  import os.path
  __usage__ = __doc__.replace("PROGRAM", os.path.basename(sys.argv[0]))
  logger = logging.getLogger()

  def die_usage(msg=""):
    sys.stderr.write("%s%s\n" % (__usage__, msg))
    sys.exit(1)

  try:
    opts, args = getopt.getopt(sys.argv[1:], "hl:vs:n:w:c:", ["help", "log=", "verbose", "storageId=", "namenode=", "command port=","web port="])
  except getopt.GetoptError, e:
    die_usage(str(e))

  logname=None
  sid="Unset"
  webport=8080
  commandport=8020
  namenode = "localhost"
  logger.setLevel(logging.INFO)
  for o, a in opts:
    if o in ("-h", "--help"): die_usage()
    if o in ("-v", "--verbose"): logger.setLevel(logging.DEBUG)
    if o in ("-l", "--log"): logname=a; print "log: [%s]" % logname
    if o in ("-s", "--storageId"): print "StorageID [%s]" % a; sid=a
    if o in ("-n", "--namenode"): print "Namenode [%s]" % a; namenode=a
    if o in ("-w", "--webport"): print "Webserver listening on port [%d]" % int(a); webport=int(a)
    if o in ("-c", "--commandport"): print "Connecting to namenode on port [%d]" % int(a); commandport=int(a)
    
  if logname: handler=logging.FileHandler(logname)
  else: handler = logging.StreamHandler(sys.stdout)
  formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
  handler.setFormatter(formatter)
  logger.addHandler(handler)

  reactor.listenTCP(webport, server.Site(datanode.DatanodeInfoResource()))

  logger.info("Registering a Datanode with StorageID %s" % (sid))
  ni = nodeinfo.NodeInfo()
  dn = datanode.DatanodeClientFactory(sid, 0, logger, ni)
  reactor.connectTCP(namenode, commandport, dn)
  logger.info("ready (Ctrl+C to stop)")
  reactor.run()
  logger.info("stopped")

