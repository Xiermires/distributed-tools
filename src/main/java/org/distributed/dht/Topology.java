package org.distributed.dht;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Topology {

    public static List<Node> getNetworkNodes(Node first) throws InterruptedException, ExecutionException {
	final List<Node> nodes = new ArrayList<>();
	nodes.add(first);
	Node curr = first.sync().getNext();
	while (!curr.equals(first)) {
	    nodes.add(curr);
	    curr = curr.sync().getNext();
	}
	return nodes;
    }

    public static void updateFingers(Node node) throws InterruptedException, ExecutionException {
	final List<Node> nodes = getNetworkNodes(node);
	final Map<BigInteger, Node> fingers = new HashMap<>();
	for (int i = 2; i < nodes.size(); i *= 2) {
	    final Node finger = nodes.get(i);
	    fingers.put(finger.getId(), finger);
	}
	final Node mid = nodes.get(nodes.size() / 2);
	fingers.put(mid.getId(), mid);
	fingers.remove(node.getId());
	node.setFingers(fingers);
    }
}
