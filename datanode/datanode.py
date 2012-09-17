#
# This code started life as example code from here:
# from http://code.activestate.com/recipes/502293-hex-dump-port-forwarding-network-proxy-server/
# Copyright (C) 2005-2007 Andrew Ellerton, mail: activestate-at->ellerton.net
#
# Per the license, the brief summary of changes are that a few utility functions were kept but otherwise
# it is new code
#

from struct import *

import google.protobuf.internal.encoder as encoder
import google.protobuf.internal.decoder as decoder

from hadoop_protocols import hadoop_rpc_pb2
from hadoop_protocols import DatanodeProtocol_pb2
from hadoop_protocols import IpcConnectionContext_pb2
from hadoop_protocols import RpcPayloadHeader_pb2
from hadoop_protocols import hdfs_pb2

try: import twisted
except ImportError:
  print "Please install the 'twisted' package from http://twistedmatrix.com/"
  sys.exit(1)

from twisted.python import failure
from twisted.internet import reactor, error, address, tcp, task
from twisted.internet.protocol import Protocol, Factory, ClientFactory 
from twisted.protocols import basic
from twisted.web import server, resource

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


def transport_info(t):
  if isinstance(t, tcp.Client): return transport_info(t.getHost())
  elif isinstance(t, address.IPv4Address): return "%s:%d" % (t.host, t.port)
  else: return repr(t)


class DatanodeClientProtocol(Protocol):
  def __init__(self, storageID, logger, nodeinfo):
    self.storageinfo = None
    self.state = True
    self._heartbeat = None
    self.heartbeatInterval = 10
    self.MAX_LENGTH = 99999
    self._unprocessed = ""
    self._nextcallid = 0
    self.calls = {}
    self.storageID = storageID
    self.logger = logger
    self.nodeinfo = nodeinfo

  def handleDNRegistrationResponse(self, message):
    rdrp = DatanodeProtocol_pb2.RegisterDatanodeResponseProto()
    rdrp.ParseFromString(message)
    self.logger.debug("Response from Namenode for registerDatanode:\n%s" % (str(rdrp)))
    self.dninfo = rdrp.registration
    self.startHeartbeat()

  def handleVersionRequestResponse(self,message):

    vrp = hdfs_pb2.VersionResponseProto()

    vrp.ParseFromString(message)
    self.logger.debug("Response from namenode for handleVersionRequest:\n%s" % (str(vrp)))
    self.storageinfo = hdfs_pb2.StorageInfoProto()
    self.storageinfo.CopyFrom(vrp.info.storageInfo)
    self.logger.debug("Storage info:\n%s" % (str(self.storageinfo)))
    self.registerDatanode()


  def handleHeartbeatResponse(self, message):
    hbrp = DatanodeProtocol_pb2.HeartbeatResponseProto()
    hbrp.ParseFromString(message)
    self.logger.debug("Response from namenode for heartbeat:\n%s" % (str(hbrp)))
  
  def sendCommand(self, request, cbinfo):
    rpchead = RpcPayloadHeader_pb2.RpcPayloadHeaderProto()

    # From the Hadoop code, some definitions:
    # enum RpcKindProto {
    # RPC_BUILTIN          = 0;  // Used for built in calls by tests
    # RPC_WRITABLE         = 1;  // Use WritableRpcEngine
    # RPC_PROTOCOL_BUFFER  = 2;  // Use ProtobufRpcEngine
    # }
    # enum RpcPayloadOperationProto {
    # RPC_FINAL_PAYLOAD        = 0; // The final payload
    # RPC_CONTINUATION_PAYLOAD = 1; // not implemented yet
    # RPC_CLOSE_CONNECTION     = 2; // close the rpc connection
    # }

    rpchead.rpcKind = 2
    rpchead.rpcOp = 0
    rpchead.callId = self._nextcallid

    self.calls[self._nextcallid] = cbinfo
    self._nextcallid += 1


    rpcreq = hadoop_rpc_pb2.HadoopRpcRequestProto()
    rpcreq.methodName = cbinfo["methodName"]
    if request is None:
      rpcreq.request = ""
    else:
      rpcreq.request =  request.SerializeToString()
    rpcreq.declaringClassProtocolName = "org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol"
    rpcreq.clientProtocolVersion = 1

    headerout = rpchead.SerializeToString()
    headerout = encoder._VarintBytes(len(headerout)) + headerout

    reqout = rpcreq.SerializeToString()
    reqout = encoder._VarintBytes(len(reqout)) + reqout

    buf = headerout + reqout

    self.transport.write(pack(">I", len(buf)))
    self.transport.write(buf)

  def registerDatanode(self):
    reg = DatanodeProtocol_pb2.RegisterDatanodeRequestProto()
    message = DatanodeProtocol_pb2.DatanodeRegistrationProto()
    message.datanodeID.ipAddr = self.nodeinfo.ipAddr
    message.datanodeID.hostName = self.nodeinfo.hostName
    message.datanodeID.storageID = self.storageID
    message.datanodeID.xferPort = self.nodeinfo.xferPort
    message.datanodeID.infoPort = self.nodeinfo.infoPort
    message.datanodeID.ipcPort = self.nodeinfo.ipcPort

    message.storageInfo.CopyFrom(self.storageinfo)

    message.keys.isBlockTokenEnabled = False
    message.keys.keyUpdateInterval = 0
    message.keys.tokenLifeTime = 0
    message.keys.currentKey.keyId = 1
    message.keys.currentKey.expiryDate = 2
    message.keys.currentKey.keyBytes = ""
    
    message.softwareVersion = "3.0.0-SNAPSHOT"

    reg.registration.CopyFrom(message)

    self.sendCommand(reg, {"name": "RegisterDatanode", "callback": self.handleDNRegistrationResponse, "methodName": "registerDatanode"})

  # 
  # The heartbeat code is mostly from the IRC protocol implementation from Twisted Words, part of the 
  # Twisted Matrix package. It's under the MIT license. 
  #
  def sendHeartbeat(self):
    hb = DatanodeProtocol_pb2.HeartbeatRequestProto()
    hb.registration.CopyFrom(self.dninfo)
    hbr = hb.reports.add()
    hbr.storageID = self.storageID
    hbr.capacity =  self.nodeinfo.capacity
    hbr.dfsUsed =  self.nodeinfo.dfsUsed
    hbr.remaining = self.nodeinfo.remaining
    hbr.blockPoolUsed =  self.nodeinfo.blockPoolUsed

    # These shouldn't be hard-coded either
    hb.xmitsInProgress = 0
    hb.xceiverCount =  1
    hb.failedVolumes = 0

    self.sendCommand(hb, {"name": "SendHeartbeat", "callback": self.handleHeartbeatResponse, "methodName": "sendHeartbeat" } )

  def _createHeartbeat(self):
      return task.LoopingCall(self._sendHeartbeat)

  def _sendHeartbeat(self):
    self.sendHeartbeat() 

  def stopHeartbeat(self):
    if self._heartbeat is not None:
      self._heartbeat.stop()
      self._heartbeat = None

  def startHeartbeat(self):
    self.logger.info("Creating a heartbeat")
    self.stopHeartbeat()
    if self.heartbeatInterval is None:
      return
    self._heartbeat = self._createHeartbeat()
    self._heartbeat.start(self.heartbeatInterval, now=False)


  def connectionMade(self):
    self.logger.debug("connection open to %s" % transport_info(self.transport))
 
    # The Hadoop Wire Protocol starts off by sending a magic number to the other side 
    # to let it know that it's establishing a connection. The first four bytes are 'hrpc'
    # (The Hadoop code checks to see if the first four bytes are instead 'GET ', in which case
    # it figures it's someone confused and pointing a web browser at the command port, in which
    # case Hadoop spits out some HTML telling them that they've made a mistake)
    #
    # From the Hadoop code:
    # +----------------------------------+
    # |  "hrpc" 4 bytes                  |
    # +----------------------------------+
    # |  Version (1 bytes)               |
    # +----------------------------------+
    # |  Authmethod (1 byte)             |
    # +----------------------------------+
    # |  IpcSerializationType (1 byte)   |
    # +----------------------------------+
    # 
    # The different versions, from src/main/java/org/apache/hadoop/ipc/Server.java
    #    1 : Introduce ping and server does not throw away RPCs
    # // 3 : Introduce the protocol into the RPC connection header
    # // 4 : Introduced SASL security layer
    # // 5 : Introduced use of {@link ArrayPrimitiveWritable$Internal}
    # //     in ObjectWritable to efficiently transmit arrays of primitives
    # // 6 : Made RPC payload header explicit
    # // 7 : Changed Ipc Connection Header to use Protocol buffers
    #
    # The auth methods are enums in src/main/java/org/apache/hadoop/security/SaslRpcServer.java
    # SIMPLE((byte) 80, "", AuthenticationMethod.SIMPLE),
    # KERBEROS((byte) 81, "GSSAPI", AuthenticationMethod.KERBEROS),
    # DIGEST((byte) 82, "DIGEST-MD5", AuthenticationMethod.TOKEN);
    # 
    # IpcSerialzationType is an enum from Server.java, with only PROTOBUF listed so far, so it's 0
    #
    header = pack('ccccbbb', 'h', 'r', 'p', 'c', 7, 80, 0)

    # That header is followed by a structure with more information about who is making the call and
    # what protocl are they asking for. Because it's unatheneticated, we pass over the effectiveUser
    # (we can't prove the real user anyway) and trust the namenode believes us. 
    ipcconn = IpcConnectionContext_pb2.IpcConnectionContextProto()
    ipcconn.userInfo.effectiveUser = "epaulson"
    ipcconn.protocol = "org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol"
    buf = ipcconn.SerializeToString()

    # Google Protocol Buffers do not specify how data should be transmitted through a stream, so everyone
    # does it a bit different. Hadoop generally uses a 4 byte int, followed by the data, and counts on the other
    # side knowing what to expect next, though on occasion it uses variable-bit length encoding.
    # (For other options, see http://stackoverflow.com/questions/11964945/canonical-way-to-transmit-protocol-buffers-over-network )

    # TODO - Handle errors
    self.transport.write(header)
    self.transport.write(pack(">I", len(buf)))
    self.transport.write(buf)
 
    # Now that we've got a connection established, we'll perform the first RPC to get started
    # 'versionRequest' doesn't take any params beyond the name, so 'None' is fine here. 
    # RPCs are given an ID in Hadoop, which is included in the response. We stash a callback for this RPC 
    # which our listener will find later.  
    self.sendCommand(None, {"name": "VersionRequest", "callback": self.handleVersionRequestResponse, "methodName": "versionRequest"})

  def write(self, buf):
    self.transport.write(buf)


  def dataReceived(self, message):
    self.logger.info("Processing incoming data of %d bytes" % (len(message))) 
    self.logger.debug("\n%s" % (hexdump("Msg", message)))

    alldata = self._unprocessed + message
    self._unprocessed = alldata
    
    while len(self._unprocessed) > 0: 
      #
      # Coming back from the NameNode will only be responses to earlier RPC Requests
      # (normally, we just send heartbeats)
      # If the NN wants us to do something, it will tack on a command then
      #
      # To decode it, we have to pick apart a few things
      # First is a variable-length encoded integer, which will tell us the size of
      # header that will follow. (This is one of the few PB things in Hadoop which are variably-encoded)
      #
      # Varints are encoded as 7 bits in a byte, with the MSB used as a flag 
      # to find the stopping position 
      # Once you get a byte with 0x80 set to 0, you've got all the bytes
      # for the encoded integer and you can decode it. 
      # 
      # Because dataReceived may give us back a partial read, we have to buffer until
      # we've read enough to make progress. I'm not being clever, and we simply buffer
      # everything, and reprocess it all as necessary 
      # 
      # for the full format, minus the bit about the varint encoded initial transmission, see  
      # hadoop-common/src/main/proto/RpcPayloadHeader.proto
      #

      # This is basically the same code that's in the PB decode routine
      position = 0
      while 1: 
         b = ord(self._unprocessed[position])
         position += 1
         if not (b & 0x80):
           break
         if position == len(self._unprocessed):
           return  

      #
      # Unfortunately, the Python implementation doesn't expose 
      # a decoding method directly for varints. 
      #
      # However, you can use an internal API to do so.
      # You get back a length, and where in the stream to start reading from
      # Thanks to Evan Jones for the info
      # http://comments.gmane.org/gmane.comp.lib.protocol-buffers.general/8223
      #
      # We could be more clever and decode it in the prior codeblock, but we'll let
      # the actual PB code do the decoding

      (size, position) = decoder._DecodeVarint(self._unprocessed,0)

      # If we don't have enough to decode yet, bail. We'll try again next time. 
      if (size + position) > len(self._unprocessed):
        return

      resp = RpcPayloadHeader_pb2.RpcResponseHeaderProto()
      resp.ParseFromString(self._unprocessed[position:position+size])

      #
      # The RpcPayloadHeader will tell us if the call was successful or unsuccessful. If it was
      # unsucessful, well, we'll figure that out later
      if resp.status == 0:
         self.logger.info("Success RPC for call %d (%s)" % (resp.callId, self.calls[resp.callId]["name"]))
      
      #
      # Success means that after we've read the header, we can read a 4-byte integer which will give
      # us the size of the next block - first, make sure we've got at least another 4 bytes already read
      #
      messagelen = 0
      if(size + position + 4) <= len(self._unprocessed):
        remaining = unpack(">I", self._unprocessed[size+position:size+position+4])
        messagelen = size + position + 4 + remaining[0] 
        #print "Still have %d bytes to process, excepting %d, and the total is %d" % (remaining[0], (size+position+4), len(alldata))
        
        #  
        # OK, we know how much farther we need to read - do we have all of that data yet? 
        # If we do, look up the function that stashed as the callback, and let it decode whatever comes next
        #
        if(messagelen) <= len(self._unprocessed):
          if "callback" in self.calls[resp.callId]:
            self.calls[resp.callId]["callback"](self._unprocessed[position+size+4:])
            self.state = False
          else:
            self.logger.info("No callback for call %d" % (resp.callId))
        
        # We know how big the response is, but we don't have all that data yet
        else: 
          return
      # We don't have all four bytes that tell us how big the response data is, so bail and try again
      else: 
        return
      
      # 
      # We're done with that callback, and have processed all of the data, so free it
      #
      del self.calls[resp.callId] 
      self._unprocessed = self._unprocessed[messagelen:]
    # end of while(_unprocessed) loop decoding messages

  def connectionLost(self, reason):
    # FIXME - reconnect or something
    self.logger.info("connection lost")


class DatanodeClientFactory(ClientFactory):

  def __init__(self, storageID, base, logger, nodeinfo):
    self.storageID = storageID
    self.base = base
    self.logger = logger
    self.nodeinfo = nodeinfo

  def startedConnecting(self, connector):
    self.logger.debug('connection opening...')

  def clientConnectionFailed(self, connector, reason):
    self.logger.info("Connection failed")

  def buildProtocol(self, addr):
    self.logger.debug('connecting to remote server %s' % transport_info(addr))
    p = DatanodeClientProtocol(self.storageID + "%s" % (self.base), self.logger, self.nodeinfo)
    self.base += 1
    return p


class DatanodeInfoResource(resource.Resource):
    
    def render_GET(self, request):
        request.setHeader("content-type", "text/plain")
        return "Someday this will have information about the current state of the daemon, but not today\n"

