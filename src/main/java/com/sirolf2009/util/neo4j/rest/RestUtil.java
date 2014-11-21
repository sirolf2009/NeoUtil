package com.sirolf2009.util.neo4j.rest;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.MediaType;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.sirolf2009.util.neo4j.NeoUtil;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class RestUtil {

	public static URI SERVER_ROOT_URI;

	public static String sendCypherRaw(String cypher) {
		final String txUri = SERVER_ROOT_URI + "transaction/commit";
		WebResource resource = Client.create().resource(txUri);
		String payload = "{\"statements\" : [ {\"statement\" : \"" +cypher + "\"} ]}";
		ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(payload).post(ClientResponse.class);
		String result = response.getEntity(String.class);
		NeoUtil.log.info(String.format("POST [%s] \n\tto [%s] \n\tstatus code [%d] \n\treturned data: \n%s", payload, txUri, response.getStatus(), result));
		response.close();
		return result;
	}

	public static JSONObject sendCypher(String cypher) {
		return (JSONObject) JSONValue.parse(sendCypherRaw(cypher));
	}

	public static void printCypher(String cypher) {
		JSONObject object = sendCypher(cypher);
		List<JSONArray> rows = JSON.getRowsFromCypherQuery(object);
		NeoUtil.log.info("-------------------------------------------");
		for(JSONArray row : rows) {
			for(Object child : row) {
				NeoUtil.log.info(child);
			}
			NeoUtil.log.info("-------------------------------------------");
		}
	}

	public static class Nodes {

		public static URI createNode() {
			final String nodeEntryPointUri = SERVER_ROOT_URI + "node";
			WebResource resource = Client.create().resource(nodeEntryPointUri);
			ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity("{}").post(ClientResponse.class);
			final URI location = response.getLocation();
			NeoUtil.log.debug(String.format("POST to [%s], status code [%d], location header [%s]",nodeEntryPointUri, response.getStatus(), location.toString()));
			response.close();
			return location;
		}

		public static URI deleteNode(URI node) {
			WebResource resource = Client.create().resource(node);
			ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).delete(ClientResponse.class);
			final URI location = response.getLocation();
			NeoUtil.log.debug(String.format("DELETE to [%s], status code [%d]",node, response.getStatus()));
			response.close();
			return location;
		}

		public static URI fromID(long ID) {
			try {
				return new URI(SERVER_ROOT_URI+"node/"+ID+"/");
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
			return null;
		}

		public static void addLabelToNode(URI node, String label) {
			final String txUri = node + (node.toString().endsWith("/") ? "labels" : "/labels");
			WebResource resource = Client.create().resource(txUri);
			String payload = "\""+label+"\"";
			ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(payload).post(ClientResponse.class);
			NeoUtil.log.debug("send payload: "+payload+" to "+txUri+" Response: "+response.getStatus());
			response.close();
		}

		public static void addPropertyToNode(URI node, String key, Object value) throws URISyntaxException {
			String propertyUri = node.toString() + (node.toString().endsWith("/") ? ("properties/"+key) : ("/properties/"+key));
			WebResource resource = Client.create().resource(propertyUri);
			String entity = value instanceof String ? "\""+value+"\"" : value.toString();
			ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(entity).put(ClientResponse.class);
			NeoUtil.log.debug(String.format("PUT to [%s], status code [%d]", propertyUri, response.getStatus()));
			response.close();
		}
		
		public static void updatePropertyFromNode(URI node, String key, Object value) {
			String propertyUri = node.toString() + (node.toString().endsWith("/") ? ("properties") : ("/properties"));
			String entity = JSON.toJsonNameValuePairCollection(key, value);
			WebResource resource = Client.create().resource(propertyUri);
			ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(entity).put(ClientResponse.class);
			NeoUtil.log.debug(String.format("PUT [%s] to [%s], status code [%d]", entity, propertyUri, response.getStatus()));
			response.close();
		}

		public static String getRawProperties(URI node) throws URISyntaxException {
			URI propertyUri = new URI(node.toString() + "properties");
			WebResource resource = Client.create().resource(propertyUri);
			ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).get(ClientResponse.class);
			String result = response.getEntity(String.class);
			NeoUtil.log.debug(String.format("POST \n\tto [%s] \n\tstatus code [%d] \n\treturned data: \n%s", propertyUri, response.getStatus(), result));
			response.close();
			return result;
		}

		public static JSONObject getProperties(URI node) throws URISyntaxException {
			return (JSONObject)JSONValue.parse(getRawProperties(node));
		}

	}

	public static class Relationship {

		public static URI addRelationship(URI startNode, URI endNode, String relationshipType, String jsonAttributes) {
			String fromUri = startNode.toString() + "/relationships";
			String relationshipJson = generateJsonRelationship(endNode, relationshipType, jsonAttributes);

			WebResource resource = Client.create().resource(fromUri);
			ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(relationshipJson).post(ClientResponse.class);

			final URI location = response.getLocation();
			NeoUtil.log.debug(String.format("POST to [%s], status code [%d], location header [%s]", fromUri, response.getStatus(), location.toString()));

			response.close();
			return location;
		}

		public static URI deleteRelationship(URI relationship) {
			WebResource resource = Client.create().resource(relationship);
			ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).delete(ClientResponse.class);
			final URI location = response.getLocation();
			NeoUtil.log.debug(String.format("DELETE to [%s], status code [%d]",relationship, response.getStatus()));
			response.close();
			return location;
		}

		public static String generateJsonRelationship(URI endNode, String relationshipType, String jsonAttributes) {
			return "{ \"to\" : \""+endNode.toString()+"\", \"type\" : \""+relationshipType+"\", \"data\" : "+jsonAttributes+" }";
		}

		public static void addMetadataToRelationship(URI relationshipUri, String name, String value) throws URISyntaxException {
			URI propertyUri = new URI(relationshipUri.toString() + "properties");
			String entity = JSON.toJsonNameValuePairCollection(name, value);
			WebResource resource = Client.create().resource(propertyUri);
			ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(entity).put(ClientResponse.class);

			NeoUtil.log.debug(String.format("PUT [%s] to [%s], status code [%d]", entity, propertyUri, response.getStatus()));
			response.close();
		}

	}

	public static class JSON {

		public static List<JSONArray> getRowsFromCypherQuery(JSONObject object) {
			List<JSONArray> rowList = new ArrayList<JSONArray>();
			JSONArray results = (JSONArray) object.get("results");
			if(results.size() > 0) {
				JSONObject data = (JSONObject) results.get(0);
				JSONArray rows = (JSONArray) data.get("data");
				for(int i = 0; i < rows.size(); i++) {
					JSONObject row = (JSONObject) rows.get(i);
					JSONArray rowData = (JSONArray) row.get("row");
					rowList.add(rowData);
				}
			} else {
				NeoUtil.log.error("No results found for object: "+object+" results:"+results);
			}
			return rowList;
		}

		public static String toJsonNameValuePairCollection(String key, Object value) {
			if(value instanceof String) {
				return "{ \""+key+"\" : \""+value+"\" }";
			} else {
				return "{ \""+key+"\" : "+value+" }";
			}
		}

	}

}
