# The Hadoop Wire Protocol: A First Experiment

A simple sketch of a how a Hadoop Datanode talks to the Namenode, in Python. Not at all useful

I'm interested in better understanding how the Hadoop daemons talk amongst themselves with their native protocols. As a first sketch, I threw together some Python to poke at the Namenode as a Datanode might. 

At the moment, it doesn't do anything especially interesting beyond registering itself with the Namenode and sending a heartbeat every 30 seconds or so. I'm not sure that it ever will do anything more, and it's unlikely that it will ever go all the way and actually store a block, much less fully respond to all possible commands from the datanode. 

It uses the the [Twisted Matrix](http://twistedmatrix.com/) framework to handle the network abstractions, so you'll need that installed before you can run this code. 

The Hadoop Protocols I'm interested in are mostly encoded in [Protocol Buffers](https://developers.google.com/protocol-buffers/), Google's data-encoding scheme. You'll need the Python Protocol Buffers library installed, as well as the code generator if you want to regenerate the Python classes that describe the messages. 

The code is spliced together from four sources:

* The structure is from [Andrew Ellerton's example proxy server](http://code.activestate.com/recipes/502293-hex-dump-port-forwarding-network-proxy-server/) for Twisted Matrix. It's under the Python Software Foundation License. 
* The Protocol Definitions, in the <code>proto/</code> directory. They're under the Apache License. The compiled versions of the definitions are in the <code>hadoop_protocols/</code> directory
* The Heartbeat code that sets up and fires heartbeats is basically taken from the Twisted IRC implementation. It's probably enough to fall under the "substantial portions" clause of the Twisted license, so see the LICENSE.Twisted for that section
* The code that finds the boundaries of the variably-encoded Protocol Buffers integers is a simplified version of the actual code from the Protocol Buffers implementation. 

The rest of the code is under no license in particular as it's not interesting enough to do anything with. Have at it!
