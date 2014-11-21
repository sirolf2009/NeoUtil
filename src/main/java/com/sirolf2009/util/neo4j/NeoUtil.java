package com.sirolf2009.util.neo4j;

import org.apache.commons.logging.impl.SimpleLog;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class NeoUtil {

	public static final SimpleLog log = new SimpleLog("NeoUtil");

	public static void registerShutdownHook(final GraphDatabaseService graphDb) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				System.out.println("Shutting Down Neo4J Database "+graphDb);
				graphDb.shutdown();
			}
		});
	}

	public static void updateNode(GraphDatabaseService graph, long ID, String key, String value) {
		try(Transaction trans = graph.beginTx()) {
			Node node = graph.getNodeById(ID);
			node.setProperty(key, value);
			trans.success();
		}
	}

}
