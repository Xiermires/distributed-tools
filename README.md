# distributed-tools
A Collection of tools that might be useful in distributed environments.

### NIO servers & conduits
Servers and communication conduits for both TCP / UDP protocols using netty and serialization via the ByteTransfer class. 

##### __TCP__
Start, stop a server by initilize / close it.
```java
final TCPServer server = new TCPServer(new TCPEcho(), "127.0.0.1", 19999); // accepting on 19999
server.close(); // close the server
```
The handler processing incoming connections is the TCPEcho instance, which in this case it echoes the request back to the sender.

Start, stop a conduit by initialize / close it.
```java
final TCPConduit conduit = new TCPConduit("127.0.0.1", 19999); // connects to server
conduit.close(); // closes the conduit
```
Conduits can be used for async request/response communication. For instance, using the echo server.
```java
try (Conduit conduit = new TCPConduit("127.0.0.1", 19999)) {
    System.out.println(conduit.send("One", String.class).get()); // prints "One"
}
```

##### __UDP__
Exactly as TCP, but bidirectional communication must be explictily stated.
```java
try (Conduit conduit = new UDPConduit("127.0.0.1", 19999, true)) { // true : bidirectional 
    System.out.println(conduit.send("One", String.class).get()); // prints "One"
}
```

### Hole punching
Hole punching allows different peers to bypass NAT firewalls by using a mediator. 

The idea is pretty simple, peers connect send UDP datagrams to the mediator, the mediator notes the sender ports and shares them with other peers.

The mediator is simply an UDPServer. Let's imagine that it has IP '192.168.96.2'.
```java
final Mediator mediator = new Mediator("192.168.96.2", 20000);
```
Now the different clients can communicate with the mediator. For instance the peer A, with IP '192.168.96.3' can register itself.
```java
try (Conduit conduit = new UDPConduit("192.168.96.2", 20000)) {  
    conduit.send(Message.hello()); 
}
```
The mediator now knows the peer A, and one accessible port. A second peer B, with IP '192.168.96.4' could now ask the mediator for peer A accessible ports to communicate with it.
```java
int port = -1;
try (Conduit conduit = new UDPConduit("192.168.96.2", 20000, true)) {  
    port = conduit.send(Message.requestPort("192.168.96.3")).get(10, TimeUnit.SECONDS); 
}

if (port == -1) {
    throw new IllegalStateException("Cannot communicate. Lost packet?");
}

final Conduit conduit = new TCPConduit("192.168.96.3", port); // open a tcp conduit
// ...
```
### Distributed hash table (DHT)
A chord-like DHT implementation is provided.

In a DHT each node has its own hash identifier and storage, and values are stored in one / other node depending on the value hash proximity to the node identifier.

Let's say that we have three peers that have started their own DHT node.
```java
// peer one
final Node nodeOne = new Node("192.168.96.2", 10001).start();
```
```java
// peer two
final Node nodeTwo = new Node("192.168.96.3", 10001).start();
```
```java
// peer three
final Node nodeThree = new Node("192.168.96.4", 10001).start();
```
Right now the nodes do not see each other. Each node must at least know one other node in the network to be able to connect to it. For instance, let's say that .3, .4 both know .2.
```java
// peer two
nodeTwo.join(new Node("192.168.96.2", 10001)); // peer two joins peer one
```
```java
// peer three
nodeThree.join(new Node("192.168.96.2", 10001)); // peer three joins peer one / peer two
```
Now that the DHT is connected, we can put / get in it similarly to in a regular Map.
```java
nodeOne.put(Keys.of("key-1"), value1)); // puts { key-1 : value1 } in the DHT (not necessarily in nodeOne)
nodeOne.get(Keys.of("key-1")); // gets value1 from somewhere in the DHT
```




