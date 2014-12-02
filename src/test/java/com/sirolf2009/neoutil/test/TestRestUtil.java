package com.sirolf2009.neoutil.test;

import static org.junit.Assert.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.impl.SimpleLog;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.sirolf2009.util.neo4j.NeoUtil;
import com.sirolf2009.util.neo4j.rest.RestAPI;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestRestUtil {

	private URI node;
	private URI node2;
	private URI node3;

	private URI relation1;
	
	private RestAPI rest;
	@Before
	public void setup() throws URISyntaxException {
		rest = new RestAPI("http://localhost:7474/db/data/");
		NeoUtil.log.setLevel(SimpleLog.LOG_LEVEL_ALL);
	}
	
	@Test
	public void testAJSONUtil() {
		System.out.println("\nPerforming json util test\n");
		
		System.out.println("Json name-value construction from key and value");
		assertEquals("Json name-value construction from key and value", "{ \"KeyTest\" : \"ValueTest\" }", rest.json.toJsonNameValuePairCollection("KeyTest", "ValueTest"));
		assertEquals("Json name-value construction from key and value", "{ \"IntTest\" : 42 }", rest.json.toJsonNameValuePairCollection("IntTest", 42));
		System.out.println();
		
		System.out.println("Json name-value construction from Map");
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put("So a zombie walks into a bar", "Through the wall");
		properties.put("I do not suffer from insanity", "I thoroughly enjoy it");
		properties.put("What is the answer to the question of the meaning of life, the universe and everything?", 42);
		assertEquals("", "{ \"So a zombie walks into a bar\" : \"Through the wall\", \"I do not suffer from insanity\" : \"I thoroughly enjoy it\", \"What is the answer to the question of the meaning of life, the universe and everything?\" : 42 }", rest.json.toJsonNameValuePairCollection(properties));
		System.out.println();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testBNodeUtil() throws URISyntaxException {		
		System.out.println("\nPerforming node util test\n");

		System.out.println("Creating node");
		node = rest.nodes.createNode();
		assertNotNull(node);
		node2 = rest.nodes.createNode();
		assertNotNull(node2);
		System.out.println();

		System.out.println("Labeling node");
		rest.nodes.addLabelToNode(node, "JUNIT_TEST_NODE");
		assertEquals("Labeling node", "{\"results\":[{\"columns\":[\"n\"],\"data\":[{\"row\":[{}]}]}],\"errors\":[]}", rest.sendCypherRaw("MATCH (n:JUNIT_TEST_NODE) RETURN n"));
		rest.nodes.addLabelToNode(node2, "JUNIT_TEST_NODE_SECOND");
		assertEquals("Labeling node", "{\"results\":[{\"columns\":[\"n\"],\"data\":[{\"row\":[{}]}]}],\"errors\":[]}", rest.sendCypherRaw("MATCH (n:JUNIT_TEST_NODE_SECOND) RETURN n"));
		System.out.println();

		System.out.println("Property-ing node with strings");
		rest.nodes.addPropertyToNode(node, "testKey", "helloWorld!");
		rest.nodes.addPropertyToNode(node2, "testKeySecond", "bye World!");
		assertEquals("Property-ing node with strings", "{\"results\":[{\"columns\":[\"n\"],\"data\":[{\"row\":[{\"testKey\":\"helloWorld!\"}]}]}],\"errors\":[]}", rest.sendCypherRaw("MATCH (n:JUNIT_TEST_NODE) RETURN n"));
		System.out.println();

		System.out.println("Property-ing node with integers");
		rest.nodes.addPropertyToNode(node2, "The_answer_to_the_ultimate_question", 42);
		assertEquals("Property-ing node with integers", "{\"results\":[{\"columns\":[\"n\"],\"data\":[{\"row\":[{\"testKeySecond\":\"bye World!\",\"The_answer_to_the_ultimate_question\":42}]}]}],\"errors\":[]}", rest.sendCypherRaw("MATCH (n:JUNIT_TEST_NODE_SECOND) RETURN n"));
		System.out.println();

		System.out.println("Setting node properties");
		rest.nodes.setNodeProperties(node2, "testKeySecond", "cya World!");
		assertEquals("Update node properties", "{\"results\":[{\"columns\":[\"n\"],\"data\":[{\"row\":[{\"testKeySecond\":\"cya World!\"}]}]}],\"errors\":[]}", rest.sendCypherRaw("MATCH (n:JUNIT_TEST_NODE_SECOND) RETURN n"));
		System.out.println();

		System.out.println("Finding node by properties");
		assertEquals("Finding node by properties", "{\"results\":[{\"columns\":[\"n\"],\"data\":[{\"row\":[{\"testKey\":\"helloWorld!\"}]}]}],\"errors\":[]}", rest.sendCypherRaw("MATCH (n) WHERE n.testKey=\\\"helloWorld!\\\" RETURN n"));
		System.out.println();

		System.out.println("Using Cypher result as JSON");
		JSONObject object = rest.sendCypher("MATCH (n:JUNIT_TEST_NODE) RETURN n");
		assertEquals("Using Cypher result as JSON", new JSONArray(), (JSONArray) object.get("errors"));
		JSONObject expectedJsonObject = new JSONObject();
		expectedJsonObject.put("testKey", "helloWorld!");
		JSONArray expectedJsonRow = new JSONArray();
		expectedJsonRow.add(expectedJsonObject);
		JSONArray[] expectedJsonArray = {expectedJsonRow};
		assertArrayEquals("Using Cypher result as JSON", expectedJsonArray, rest.json.getRowsFromCypherQuery(object).toArray());
		System.out.println();

		System.out.println("Testing multple column cypher results");
		node3 = rest.nodes.createNode();
		rest.nodes.addLabelToNode(node3, "JUNIT_TEST_NODE");
		rest.nodes.addPropertyToNode(node3, "Third", "Node");
		assertEquals("Testing multiple column cypher results", "[[{\"testKey\":\"helloWorld!\"},{\"testKeySecond\":\"cya World!\"}], [{\"Third\":\"Node\"},{\"testKeySecond\":\"cya World!\"}]]", rest.json.getRowsFromCypherQuery(rest.sendCypher("MATCH (n:JUNIT_TEST_NODE), (m:JUNIT_TEST_NODE_SECOND) RETURN n, m")).toString());
		System.out.println();

		System.out.println("Deleting nodes");
		rest.nodes.deleteNode(node);
		rest.nodes.deleteNode(node2);
		rest.nodes.deleteNode(node3);
		assertEquals("Deleting nodes", "{\"results\":[{\"columns\":[\"n\"],\"data\":[]}],\"errors\":[]}", rest.sendCypherRaw("MATCH (n:JUNIT_TEST_NODE) RETURN n"));
		System.out.println();
	}

	@Test
	public void testCRelationshipUtil() throws URISyntaxException {
		System.out.println("\nPerforming relationship util test\n");

		node = rest.nodes.createNode();
		node2 = rest.nodes.createNode();
		rest.nodes.addLabelToNode(node, "JUNIT_TEST_NODE");
		rest.nodes.addLabelToNode(node2, "JUNIT_TEST_NODE_SECOND");

		System.out.println("Adding a relatonship");
		relation1 = rest.relationship.addRelationship(node, node2, "JUNIT_TEST_RELATIONSHIP");
		assertEquals("Adding a relationship", "{\"results\":[{\"columns\":[\"r\"],\"data\":[{\"row\":[{}]},{\"row\":[{}]}]}],\"errors\":[]}", rest.sendCypherRaw("MATCH ()-[r:JUNIT_TEST_RELATIONSHIP]-() RETURN r"));
		System.out.println();
		
		System.out.println("Setting string property to relationship");
		rest.relationship.setRelationshipProperties(relation1, "testKey", "value");
		assertEquals("Setting string property to relationship", "{\"results\":[{\"columns\":[\"r\"],\"data\":[{\"row\":[{\"testKey\":\"value\"}]},{\"row\":[{\"testKey\":\"value\"}]}]}],\"errors\":[]}", rest.sendCypherRaw("MATCH ()-[r:JUNIT_TEST_RELATIONSHIP]-() RETURN r"));
		System.out.println();
		
		System.out.println("Setting integer property to relationship");
		rest.relationship.setRelationshipProperties(relation1, "The answer to the ultimate question", 42);
		assertEquals("Setting string property to relationship", "{\"results\":[{\"columns\":[\"r\"],\"data\":[{\"row\":[{\"The answer to the ultimate question\":42}]},{\"row\":[{\"The answer to the ultimate question\":42}]}]}],\"errors\":[]}", rest.sendCypherRaw("MATCH ()-[r:JUNIT_TEST_RELATIONSHIP]-() RETURN r"));
		System.out.println();
		
		System.out.println("Setting string and integer properties to relationship");
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put("testKey", "value");
		properties.put("The answer to the ultimate question", 42);
		rest.relationship.setRelationshipProperties(relation1, properties);
		assertEquals("Setting string and integer properties to relationship", "{\"results\":[{\"columns\":[\"r\"],\"data\":[{\"row\":[{\"testKey\":\"value\",\"The answer to the ultimate question\":42}]},{\"row\":[{\"testKey\":\"value\",\"The answer to the ultimate question\":42}]}]}],\"errors\":[]}", rest.sendCypherRaw("MATCH ()-[r:JUNIT_TEST_RELATIONSHIP]-() RETURN r"));
		System.out.println();

		System.out.println("Deleting a relationship");
		rest.relationship.deleteRelationship(relation1);
		assertEquals("Deleting a relationship", "{\"results\":[{\"columns\":[\"r\"],\"data\":[]}],\"errors\":[]}", rest.sendCypherRaw("MATCH ()-[r:JUNIT_TEST_RELATIONSHIP]-() RETURN r"));
		System.out.println();
		
		rest.nodes.deleteNode(node);
		rest.nodes.deleteNode(node2);
	}

	@After
	public void cleanup() {
		System.out.println("Cleaning up\n");
		if(relation1 != null) {
			rest.relationship.deleteRelationship(relation1);
		}
		
		if(node != null) {
			rest.nodes.deleteNode(node);
		}
		if(node2 != null) {
			rest.nodes.deleteNode(node2);
		}
		if(node3 != null) {
			rest.nodes.deleteNode(node3);
		}
	}

}
