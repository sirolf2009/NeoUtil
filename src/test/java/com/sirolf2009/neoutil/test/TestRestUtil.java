package com.sirolf2009.neoutil.test;

import static com.sirolf2009.util.neo4j.rest.RestUtil.*;
import static com.sirolf2009.util.neo4j.rest.RestUtil.Nodes.*;
import static com.sirolf2009.util.neo4j.rest.RestUtil.JSON.*;
import static org.junit.Assert.*;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.logging.impl.SimpleLog;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sirolf2009.util.neo4j.NeoUtil;

public class TestRestUtil {

	private URI node;
	private URI node2;
	private URI node3;

	@Before
	public void setup() throws URISyntaxException {
		SERVER_ROOT_URI = new URI("http://localhost:7474/db/data/");
		NeoUtil.log.setLevel(SimpleLog.LOG_LEVEL_ALL);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testNodeUtil() throws URISyntaxException {		
		System.out.println("Creating node");
		node = createNode();
		assertNotNull(node);
		node2 = createNode();
		assertNotNull(node2);

		System.out.println("Labeling node");
		addLabelToNode(node, "JUNIT_TEST_NODE");
		assertEquals("Labeling node", "{\"results\":[{\"columns\":[\"n\"],\"data\":[{\"row\":[{}]}]}],\"errors\":[]}", sendCypherRaw("MATCH (n:JUNIT_TEST_NODE) RETURN n"));
		addLabelToNode(node2, "JUNIT_TEST_NODE_SECOND");
		assertEquals("Labeling node", "{\"results\":[{\"columns\":[\"n\"],\"data\":[{\"row\":[{}]}]}],\"errors\":[]}", sendCypherRaw("MATCH (n:JUNIT_TEST_NODE_SECOND) RETURN n"));
	
		//TODO Integers and such
		System.out.println("Property-ing node");
		addPropertyToNode(node, "testKey", "helloWorld!");
		addPropertyToNode(node2, "testKeySecond", "bye World!");
		assertEquals("Property-ing node", "{\"results\":[{\"columns\":[\"n\"],\"data\":[{\"row\":[{\"testKey\":\"helloWorld!\"}]}]}],\"errors\":[]}", sendCypherRaw("MATCH (n:JUNIT_TEST_NODE) RETURN n"));
		
		System.out.println("Update node properties");
		updatePropertyFromNode(node2, "testKeySecond", "cya World!");
		assertEquals("Update node properties", "{\"results\":[{\"columns\":[\"n\"],\"data\":[{\"row\":[{\"testKeySecond\":\"cya World!\"}]}]}],\"errors\":[]}", sendCypherRaw("MATCH (n:JUNIT_TEST_NODE_SECOND) RETURN n"));
		
		System.out.println("Finding node by properties");
		assertEquals("Finding node by properties", "{\"results\":[{\"columns\":[\"n\"],\"data\":[{\"row\":[{\"testKey\":\"helloWorld!\"}]}]}],\"errors\":[]}", sendCypherRaw("MATCH (n) WHERE n.testKey=\\\"helloWorld!\\\" RETURN n"));
		
		System.out.println("Using Cypher result as JSON");
		JSONObject object = sendCypher("MATCH (n:JUNIT_TEST_NODE) RETURN n");
		assertEquals("Using Cypher result as JSON", new JSONArray(), (JSONArray) object.get("errors"));
		JSONObject expectedJsonObject = new JSONObject();
		expectedJsonObject.put("testKey", "helloWorld!");
		JSONArray expectedJsonRow = new JSONArray();
		expectedJsonRow.add(expectedJsonObject);
		JSONArray[] expectedJsonArray = {expectedJsonRow};
		assertArrayEquals("Using Cypher result as JSON", expectedJsonArray, getRowsFromCypherQuery(object).toArray());

		System.out.println("Testing multple column cypher results");
		node3 = createNode();
		addLabelToNode(node3, "JUNIT_TEST_NODE");
		addPropertyToNode(node3, "Third", "Node");
		assertEquals("Testing multiple column cypher results", "[[{\"testKey\":\"helloWorld!\"},{\"testKeySecond\":\"cya World!\"}], [{\"Third\":\"Node\"},{\"testKeySecond\":\"cya World!\"}]]", getRowsFromCypherQuery(sendCypher("MATCH (n:JUNIT_TEST_NODE), (m:JUNIT_TEST_NODE_SECOND) RETURN n, m")).toString());

		System.out.println("Deleting nodes");
		deleteNode(node);
		deleteNode(node2);
		deleteNode(node3);
		assertEquals("Deleting nodes", "{\"results\":[{\"columns\":[\"n\"],\"data\":[]}],\"errors\":[]}", sendCypherRaw("MATCH (n:JUNIT_TEST_NODE) RETURN n"));
	}
	
	@After
	public void cleanup() {
		deleteNode(node);
		deleteNode(node2);
		deleteNode(node3);
	}

}
