#!/usr/bin/env python
# from http://code.activestate.com/recipes/502293-hex-dump-port-forwarding-network-proxy-server/
# hex-dump network proxy: run with "-h" to see usage information
"""usage: PROGRAM [options] <fromport:hostname:toport> <...>

A port forwarding proxy server that (optionally) hex-dumps traffic 
in both directions.  Augmented to understand Hadoop 
Datanode<->Namenode traffic

Example:
  Listen for and forward connections on port 8021 to port 8020 on
  namenode.yourdomain.com, hex-dumping the network traffic in both directions:
 
  $ PROGRAM 8081:namenode.yourdomain.com:8021 -v

  Options:
  "-h", "--help": This message
  "-v", "--verbose": Expanded information, including hex traffic
  "-l foo", "--log=foo": Store results in 'foo' rather than stdout
  "-r", "--rawLogging": Create datafiles with traffic capture

Raw logged files are formatted as:

+----------------------------------+
| 4 byte integer of Segment Length |
+----------------------------------+
| Segment Length of Data           |
+----------------------------------+
| 4 byte integer                   |
+----------------------------------+
| Segment Length of Data           |
+----------------------------------+

Concatenating all segments together is the data stream. 

The segments roughly correspond to the boundaries that the 
other end of the connection transmitted data as an individual
operation, though due to buffering in the various layers and
intermediate devices there is no guarantee than any segment
represents a single atomic unit. 

Original Code Copyright (C) 2005-2007 Andrew Ellerton 
mail: activestate-at->ellerton.net
"""
import sys, getopt, logging, re, time
try: import twisted
except ImportError:
  print "Please install the 'twisted' package from http://twistedmatrix.com/"
  sys.exit(1)
from struct import *
from twisted.python import failure
from twisted.internet import reactor, error, address, tcp
from twisted.internet.protocol import Protocol, Factory, ClientFactory

import google.protobuf.internal.encoder as encoder
import google.protobuf.internal.decoder as decoder

from hadoop_protocols import hadoop_rpc_pb2
from hadoop_protocols import DatanodeProtocol_pb2
from hadoop_protocols import IpcConnectionContext_pb2
from hadoop_protocols import RpcPayloadHeader_pb2


# A decorator function to generate a generator function
# From http://eli.thegreenplace.net/2009/08/29/co-routines-as-an-alternative-to-state-machines/
def coroutine(func):
    def start(*args,**kwargs):
        cr = func(*args,**kwargs)
        cr.next()
        return cr
    return start

# --------------------------------------------------------------------------
# This GREAT hexdump function from Sebastian Keim's Python recipe at:
# http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/142812
# --------------------------------------------------------------------------
HEX_FILTER=''.join([(len(repr(chr(x)))==3) and chr(x) or '.' for x in range(256)])
def hexdump(prefix, src, length=16):
  N=0; result=''
  while src:
    s,src = src[:length],src[length:]
    hexa = ' '.join(["%02X"%ord(x) for x in s])
    s = s.translate(HEX_FILTER)
    result += "%s %04X   %-*s   %s\n" % (prefix, N, length*3, hexa, s)
    N+=length
  return result
# --------------------------------------------------------------------------
def transport_info(t):
  if isinstance(t, tcp.Client): return transport_info(t.getHost())
  elif isinstance(t, address.IPv4Address): return "%s:%d" % (t.host, t.port)
  else: return repr(t)

class MessageLogger:
  def __init__(self, logname, startLogging=False):
    self.loggingEnabled = startLogging
    self.output = None
    if self.loggingEnabled:
      self.output = open(str(logname), "wb")    

  def enableLogging(self):
    # TODO - Make sure that we open up the output file!
    self.loggingEnabled = True
   
  def disableLogging(self):
    self.output.close()
    self.output = None
    self.loggingEnabled = False

  def store(self, data):
    if self.loggingEnabled:
      self.output.write(pack(">I", len(data))) 
      self.output.write(data)

  def cleanup(self):
    if self.output:
      self.output.close()

class HexdumpClientProtocol(Protocol):
  def __init__(self, factory):
    self.factory=factory

  def connectionMade(self):
    logger.debug("bridge connection open %s" % transport_info(self.transport))
    self.factory.owner.clientName=transport_info(self.transport)
    self.writeCache()

  def write(self, buf):
    self.transport.write(buf)

  def dataReceived(self, recvd):
    self.factory.owner.write(recvd)

  def writeCache(self):
    while self.factory.writeCache:
      item = self.factory.writeCache.pop(0)
      self.write(item)

class HexdumpClientFactory(ClientFactory):
  def __init__(self, owner):
    self.owner = owner
    self.writeCache = []

  def startedConnecting(self, connector):
    logger.debug('connection opening...')

  def buildProtocol(self, addr):
    logger.debug('connecting to remote server %s' % transport_info(addr))
    p = HexdumpClientProtocol(self)
    self.owner.dest = p
    return p

  def clientConnectionLost(self, connector, reason):
    if isinstance(reason, failure.Failure):
      if reason.type == twisted.internet.error.ConnectionDone:
        logger.info("remote server closed connection")
      else:
        logger.info("remote server connection lost, reason: %r" % reason)
    self.owner.close()

  def clientConnectionFailed(self, connector, reason):
    logger.debug("dest: connection failed: %r" % reason)
    self.owner.close()

class HexdumpServerProtocol(Protocol):

  @coroutine 
  def process_incoming_data(self):
    message = ""
    while True:
      message = message + (yield)
      if(len(message) >= 7):
        (header, v, a, i) = unpack_from('4sbbb', message[0:7])
        logger.debug("Header decoded: %s %d %d %d" % (header, v, a, i))
        message = message[7:]
        while True:
          if(len(message) >= 4):
            size = unpack(">I", message[0:4])
            message = message[4:]
            while True:
              if(len(message) >= size[0]): 
                 ipcconn = IpcConnectionContext_pb2.IpcConnectionContextProto()
                 ipcconn.ParseFromString(message[0:size[0]])
                 logger.info("Logging a connection:\n%s" % (str(ipcconn)))
                 message = message[size[0]:]

                 while True:
                   if(len(message) >= 4):
                     size = unpack(">I", message[0:4])
                     #message = message[4:]
                     while True:
                       if(len(message) >= size[0]): 
                         rpchead = RpcPayloadHeader_pb2.RpcPayloadHeaderProto()
                         (headersize, headerposition) = decoder._DecodeVarint(message[4:],0)
                         rpchead.ParseFromString(message[4+headerposition:4+headerposition+headersize])
                         message = message[4+headerposition+headersize:]
                         (bodysize, bodyposition) = decoder._DecodeVarint(message[0:],0)
                         rpcreq = hadoop_rpc_pb2.HadoopRpcRequestProto()
                         rpcreq.ParseFromString(message[bodyposition:bodyposition+bodysize])
                         logger.info("The Request is for %s" % (rpcreq.methodName))
                         self.messages[rpchead.callId] = {"time": time.time(), "method": rpcreq.methodName}
                         message = ""
                       else:
                         message = message + (yield)
                   else:
                     message = message + (yield)
                  # End of the RpcReq Processing

              else:
                message = message + (yield)
              # End of waiting for data for IPC Header

          else:
            message = message + (yield) 
           # End of waiting for enough data for the initial header

     # self.messageCount += 1

  @coroutine
  def process_outgoing_data(self):
    message = ""
    while True:
      position = 0
      while True:
        message = message + (yield)
        if position == len(message):
          next
        b = ord(message[position])
        position += 1
        if not (b & 0x80):
          break

      # At this point, we know we've got enough data onhand to decode the length integer, so
      # we don't have to flip back to the other function to get more data
      (msgsize, msgstart) = decoder._DecodeVarint(message, 0)  
      
      # FIXME - this didn't work, figure out what went wrong
      #message = message[msgstart:]

      # Keep looping until we've got enough data to decode the RPC results
      while True:
        if(len(message) >= msgsize-msgstart):
          resp = RpcPayloadHeader_pb2.RpcResponseHeaderProto()
          #logger.info("We've got %d bytes, decoding a message of %d starting at %d" % (len(message), msgsize, msgstart))
          resp.ParseFromString(message[msgstart:msgstart+msgsize])
          elapsed = time.time() - self.messages[resp.callId]["time"]
          if resp.status == 0:
            logger.info("Success for RPC %d (%s) - took %.4f seconds" % (resp.callId, self.messages[resp.callId]["method"], elapsed))
          message = message[msgsize+msgstart:]

          while True:
            if(len(message) >= 4):
              remaining = unpack(">I", message[0:4])
              message = message[4:]
              while True:
                if(len(message) >= remaining[0]):
                  logger.debug("Read all of the response bytes, but don't really care what it is")
                  message = message[remaining[0]:]
                  break
                else:
                  logger.debug("Reading more data for full response, have %d, waiting for %d" % (len(message), remaining[0]))
                  message = message + (yield)
              break
            else:
              logger.debug("Reading more data for length of full response")
              message = message + (yield)
          break
        else:
          message = message + (yield)

  def __init__(self, serverFactory):
    self.factory=serverFactory
    self.clientFactory = HexdumpClientFactory(self)
    self.clientName=id(self) # this is repopulated later
    self.serverName=None
    self.clientData=None
    self.serverData=None
    self.messages = {}
    self.incomingprocessor = self.process_incoming_data()
    self.outgoingprocessor = self.process_outgoing_data()

  def enableLogging(self):
    self.clientData.enableLogging()
    self.serverData.enableLogging()
  
  def disableLogging(self):
    self.clientData.disableLogging()
    self.serverData.disableLogging()

  def serverName(self):
    return "%s:%d" %(self.factory.remote_host,self.factory.remote_port)

  def connectionMade(self):
    self.serverName="%s:%d" %(self.factory.remote_host,self.factory.remote_port)
    logger.info("client %s opened connection -> server %s" % (
      self.clientName, self.serverName))

    # cxn to this server has opened. Open a port to the destination...
    reactor.connectTCP(self.factory.remote_host,
      self.factory.remote_port, self.clientFactory)

    self.clientData = MessageLogger("%s" % (self.clientName), self.factory.raw_logging)
    self.serverData = MessageLogger("%s" % (self.serverName), self.factory.raw_logging) 

  def connectionLost(self, reason):
    logger.info("client %s closed connection" % self.clientName) # str(reason)
    if self.dest and self.dest.transport: self.dest.transport.loseConnection()
    self.incomingprocessor.close()

  def connectionFailed(self, reason):
    logger.debug("proxy connection failed: %s" % str(reason))

  def dataReceived(self, recvd):
    logger.info("client %s -> server %s (%d bytes)" % (
      self.clientName, self.serverName, len(recvd)))
    logger.debug("\n%s" % (hexdump('->', recvd)))
    self.incomingprocessor.send(recvd)
    if hasattr(self, "dest"):
      self.dest.write(recvd)
      self.clientData.store(recvd)
    else:
      logger.debug("caching data until remote connection is open")
      self.clientData.store(recvd)
      self.clientFactory.writeCache.append(recvd)

  def write(self, buf):
    logger.info("client %s <= server %s (%d bytes)" % (
      self.clientName, self.serverName, len(buf)))
    logger.debug("\n%s" % (hexdump('<=', buf)))
    self.transport.write(buf)
    self.outgoingprocessor.send(buf)
    self.serverData.store(buf)

  def close(self):
    self.dest = None
    self.transport.loseConnection()
    self.clientData.cleanup()
    self.serverData.cleanup()

class HexdumpServerFactory(Factory):
  def __init__(self, listen_port, remote_host, remote_port, rawLogging = False, max_connections = None):
    self.listen_port = listen_port
    self.remote_host = remote_host
    self.remote_port = remote_port
    self.raw_logging = rawLogging
    self.max_connections = max_connections
    self.numConnections = 0

  def startFactory(self):
    logger.info("listening on %d -> %s:%d" % (self.listen_port, self.remote_host, self.remote_port))

  def buildProtocol(self, addr): # could process max/num connections here
    return HexdumpServerProtocol(self)

if __name__ == "__main__":
  import os.path
  __usage__ = __doc__.replace("PROGRAM", os.path.basename(sys.argv[0]))
  port_map_pattern=re.compile("(\d+):([\w\.\-]+):(\d+)")
  logger = logging.getLogger()
  num_factories=0

  def die_usage(msg=""):
    sys.stderr.write("%s%s\n" % (__usage__, msg))
    sys.exit(1)

  def add_factory(port_map_desc, rawLogging):
    match = port_map_pattern.match(port_map_desc)
    if not match: die_usage("malformed port map description: %s" % port_map_desc)
    listen_port = int(match.group(1))
    remote_host = match.group(2)
    remote_port = int(match.group(3))
    factory = HexdumpServerFactory(listen_port, remote_host, remote_port, rawLogging)
    reactor.listenTCP(factory.listen_port, factory)

  try:
    opts, args = getopt.getopt(sys.argv[1:], "hl:vr:", ["help", "log=", "verbose", "raw logging"])
  except getopt.GetoptError, e:
    die_usage(str(e))

  logname=None
  rawLogging = False
  logger.setLevel(logging.INFO)
  for o, a in opts:
    if o in ("-h", "--help"): die_usage()
    if o in ("-v", "--verbose"): logger.setLevel(logging.DEBUG)
    if o in ("-l", "--log"): logname=a; print "log: [%s]" % logname
    if o in ("-r", "--rawLogging"): rawLogging = True

  if logname: handler=logging.FileHandler(logname)
  else: handler = logging.StreamHandler(sys.stdout)
  formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
  handler.setFormatter(formatter)
  logger.addHandler(handler)

  for a in args: add_factory(a, rawLogging); num_factories+=1
  if num_factories==0: die_usage("No proxy/port forwarding connections specified")

  logger.info("ready (Ctrl+C to stop)")
  reactor.run()
  logger.info("stopped")
