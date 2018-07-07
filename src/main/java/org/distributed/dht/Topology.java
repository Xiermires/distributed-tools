package org.distributed.dht;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.Ordering;

public class Topology {

    public static List<Node> getNetworkNodes(Node connectedNode) throws InterruptedException, ExecutionException {
	final List<Node> nodes = new ArrayList<>();
	nodes.add(connectedNode);
	Node curr = connectedNode.sync().getNext();
	while (!curr.equals(connectedNode)) {
	    nodes.add(curr);
	    curr = curr.sync().getNext();
	}
	return nodes;
    }

    public static void updateFingers(Node node) throws InterruptedException, ExecutionException {
	final List<Node> nodes = getNetworkNodes(node);
	Collections.sort(nodes, Ordering.natural());

	final int pos = nodes.indexOf(node);
	final Map<BigInteger, Node> fingers = new HashMap<>();
	for (int i = pos + 2, inc = 2; i < nodes.size(); i += inc, inc *= 2) {
	    final Node finger = nodes.get(i);
	    fingers.put(finger.getId(), finger);
	}	
	for (int i = pos, inc = 2; i >= 0; i -= inc, inc *= 2) {
	    final Node finger = nodes.get(i);
	    fingers.put(finger.getId(), finger);
	}
	
	final Node mid = nodes.get(nodes.size() / 2);
	fingers.put(mid.getId(), mid);
	fingers.remove(node.getId());
	node.setFingers(fingers);
	node.sync(node);
    }
}
