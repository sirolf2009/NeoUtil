package com.sirolf2009.util.neo4j.cypher;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Node;
import org.neo4j.helpers.collection.IteratorUtil;

import com.sirolf2009.util.neo4j.NeoUtil;

public abstract class CypherHelper {

	public static Node FindNode(ExecutionEngine engine, String label) {
		return FindNodeByProperties(engine, label, null);
	}

	public static List<Node> FindNodes(ExecutionEngine engine, String label) {
		return FindNodesByProperties(engine, label, null);
	}

	public static Node FindNodeByProperties(ExecutionEngine engine, String label, Map<String, Object> properties) {
		List<Node> nodes = FindNodesByProperties(engine, label, properties);
		if(nodes != null && nodes.size() > 0) {
			return nodes.get(0);
		}
		return null;
	}

	public static List<Node> FindNodesByProperties(ExecutionEngine engine, String label, Map<String, Object> properties) {
		List<List<Node>> nodes = FindNodesByPropertiesRelation(engine, label, properties, null);
		if(nodes.size() > 0) {
			return FindNodesByPropertiesRelation(engine, label, properties, null).get(0);
		}
		return null;
	}

	public static List<Node> FindNodesByRelation(ExecutionEngine engine, String label, String relation) {
		return FindNodesByPropertiesRelation(engine, label, null, relation).get(0);
	}

	public static Node FindNodeByRelation(ExecutionEngine engine, String label, String relation) {
		List<List<Node>> nodes = FindNodesByPropertiesRelation(engine, label, null, relation);
		if(nodes.size() > 0) {
			return FindNodesByPropertiesRelation(engine, label, null, relation).get(0).get(0);
		}
		return null;
	}

	public static List<List<Node>> FindNodesByPropertiesRelation(ExecutionEngine engine, String label, Map<String, Object> properties, String relation) {
		ExecutionResult result;
		if(properties != null) {
			String props = getPropertiesFromMap(properties);
			if(relation != null && !relation.isEmpty()) {
				result = Cypher(engine, "MATCH (result:"+label+" "+props+")<-[:"+relation+"]-(actor) return result");
			} else {
				result = Cypher(engine, "MATCH (result:"+label+" "+props+") return result");
			}
		} else {
			if(relation != null && !relation.isEmpty()) {
				result = Cypher(engine, "MATCH (result:"+label+")<-[:"+relation+"]-(actor) return result");
			} else {
				result = Cypher(engine, "MATCH (result:"+label+") return result");
			}
		}

		return GetNodesFromResult(result);
	}

	public static String getPropertiesFromMap(Map<String, Object> properties) {
		if(properties == null) {
			throw new IllegalArgumentException("properties cannot be null");
		}
		String props = "{";
		for(String key : properties.keySet()) {
			props += key+": '"+properties.get(key)+"'";
		}
		props += "}";
		return props;
	}

	public static List<List<Node>> GetAllNodes(ExecutionEngine engine) {
		return GetNodesFromResult(Cypher(engine, "MATCH(result) return result"));
	}

	public static ExecutionResult Cypher(ExecutionEngine engine, String cypher) {
		NeoUtil.log.info("Sending Cypher: "+cypher);
		ExecutionResult result = engine.execute(cypher);
		//TODO send result.getQueryStatistics() to analyzer
		return result;
	}

	public static List<List<Node>> GetNodesFromResult(ExecutionResult result, String... columnNames) {
		if(columnNames == null || columnNames.length == 0) {
			result.columns().toArray(columnNames);
		}
		List<List<Node>> columns = new ArrayList<List<Node>>();
		for(String columnName : columnNames) {
			List<Node> nodes = new ArrayList<Node>();
			Iterator<Node> columnResult = result.columnAs(columnName);
			for (Node node : IteratorUtil.asIterable(columnResult)) {
				nodes.add(node);
			}
			columns.add(nodes);
		}
		return columns;
	}

}
