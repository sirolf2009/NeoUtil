package com.sirolf2009.util.neo4j.rest;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.sirolf2009.util.neo4j.NeoUtil;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class RestAPI {

	public URI SERVER_ROOT_URI;
	public Nodes nodes;
	public Relationship relationship;
	public JSON json;
	
	public RestAPI(String uri) throws URISyntaxException {
		this(new URI(uri));
	}
	
	public RestAPI(URI uri) {
		SERVER_ROOT_URI = uri;
		nodes = new Nodes(this);
		relationship = new Relationship(this);
		json = new JSON(this);
	}

	public String sendCypherRaw(String cypher) {
		final String txUri = SERVER_ROOT_URI + "transaction/commit";
		WebResource resource = Client.create().resource(txUri);
		String payload = "{\"statements\" : [ {\"statement\" : \"" +cypher + "\"} ]}";
		ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(payload).post(ClientResponse.class);
		String result = response.getEntity(String.class);
		NeoUtil.log.info(String.format("POST [%s] \n\tto [%s] \n\tstatus code [%d] \n\treturned data: \n%s", payload, txUri, response.getStatus(), result));
		response.close();
		return result;
	}

	public JSONObject sendCypher(String cypher) {
		return (JSONObject) JSONValue.parse(sendCypherRaw(cypher));
	}

	public void printCypher(String cypher) {
		JSONObject object = sendCypher(cypher);
		List<JSONArray> rows = json.getRowsFromCypherQuery(object);
		NeoUtil.log.info("-------------------------------------------");
		for(JSONArray row : rows) {
			for(Object child : row) {
				NeoUtil.log.info(child);
			}
			NeoUtil.log.info("-------------------------------------------");
		}
	}

	private String extendNode(URI node, String extension) {
		return extendNode(node.toString(), extension);
	}

	private String extendNode(String node, String extension) {
		return node.endsWith("/") ? node + extension : node + "/" + extension;
	}

	public class Nodes {
		
		public RestAPI restUtil;
		
		public Nodes(RestAPI restUtil) {
			this.restUtil = restUtil;
		}

		public URI createNode() {
			final String nodeEntryPointUri = extendNode(SERVER_ROOT_URI, "node");
			WebResource resource = Client.create().resource(nodeEntryPointUri);
			ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity("{}").post(ClientResponse.class);
			final URI location = response.getLocation();
			NeoUtil.log.debug(String.format("POST to [%s], status code [%d], location header [%s]",nodeEntryPointUri, response.getStatus(), location.toString()));
			response.close();
			return location;
		}

		public URI deleteNode(URI node) {
			WebResource resource = Client.create().resource(node);
			ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).delete(ClientResponse.class);
			final URI location = response.getLocation();
			NeoUtil.log.debug(String.format("DELETE to [%s], status code [%d]",node, response.getStatus()));
			response.close();
			return location;
		}

		public URI fromID(long ID) {
			try {
				return new URI(extendNode(SERVER_ROOT_URI, "node/"+ID));
			} catch (URISyntaxException e) {
				NeoUtil.log.error("Could not get node with ID:"+ID, e);
			}
			return null;
		}

		public void addLabelToNode(URI node, String label) {
			final String txUri = extendNode(node, "labels");
			WebResource resource = Client.create().resource(txUri);
			String payload = "\""+label+"\"";
			ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(payload).post(ClientResponse.class);
			NeoUtil.log.debug("send payload: "+payload+" to "+txUri+" Response: "+response.getStatus());
			response.close();
		}

		public void addPropertyToNode(URI node, String key, Object value) throws URISyntaxException {
			String propertyUri = extendNode(node, "properties/"+key);
			WebResource resource = Client.create().resource(propertyUri);
			String entity = value instanceof String ? "\""+value+"\"" : value.toString();
			ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(entity).put(ClientResponse.class);
			NeoUtil.log.debug(String.format("PUT to [%s], status code [%d]", propertyUri, response.getStatus()));
			response.close();
		}

		public void setNodeProperties(URI node, String key, Object value) {
			setNodeProperties(node, restUtil.json.toJsonNameValuePairCollection(key, value));
		}

		public void setNodeProperties(URI node, String jsonProperties) {
			String propertyUri = extendNode(node, "properties");
			WebResource resource = Client.create().resource(propertyUri);
			ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(jsonProperties).put(ClientResponse.class);
			NeoUtil.log.debug(String.format("PUT [%s] to [%s], status code [%d]", jsonProperties, propertyUri, response.getStatus()));
			response.close();
		}

		public String getRawProperties(URI node) throws URISyntaxException {
			URI propertyUri = new URI(extendNode(node, "properties"));
			WebResource resource = Client.create().resource(propertyUri);
			ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).get(ClientResponse.class);
			String result = response.getEntity(String.class);
			NeoUtil.log.debug(String.format("POST \n\tto [%s] \n\tstatus code [%d] \n\treturned data: \n%s", propertyUri, response.getStatus(), result));
			response.close();
			return result;
		}

		public JSONObject getProperties(URI node) throws URISyntaxException {
			return (JSONObject)JSONValue.parse(getRawProperties(node));
		}

	}

	public class Relationship {
		
		public RestAPI restUtil;
		
		public Relationship(RestAPI restUtil) {
			this.restUtil = restUtil;
		}

		public URI addRelationship(URI startNode, URI endNode, String relationshipType) {
			String fromUri = extendNode(startNode, "relationships");
			String relationshipJson = generateJsonRelationship(endNode, relationshipType);

			WebResource resource = Client.create().resource(fromUri);
			ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(relationshipJson).post(ClientResponse.class);

			final URI location = response.getLocation();
			NeoUtil.log.debug(String.format("POST [%s] to [%s], status code [%d], location header [%s]", relationshipJson, fromUri, response.getStatus(), location));

			response.close();
			return location;
		}

		public URI addRelationshipWithProperties(URI startNode, URI endNode, String relationshipType, String jsonAttributes) {
			String fromUri = extendNode(startNode, "relationships");
			String relationshipJson = generateJsonRelationshipWithAttributes(endNode, relationshipType, jsonAttributes);

			WebResource resource = Client.create().resource(fromUri);
			ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(relationshipJson).post(ClientResponse.class);

			final URI location = response.getLocation();
			NeoUtil.log.debug(String.format("POST [%s] to [%s], status code [%d], location header [%s]", relationshipJson, fromUri, response.getStatus(), location));

			response.close();
			return location;
		}

		public URI deleteRelationship(URI relationship) {
			WebResource resource = Client.create().resource(relationship);
			ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).delete(ClientResponse.class);
			final URI location = response.getLocation();
			NeoUtil.log.debug(String.format("DELETE to [%s], status code [%d]",relationship, response.getStatus()));
			response.close();
			return location;
		}

		public String generateJsonRelationshipWithAttributes(URI endNode, String relationshipType, String jsonAttributes) {
			return "{ \"to\" : \""+endNode.toString()+"\", \"type\" : \""+relationshipType+"\", \"data\" : "+jsonAttributes+" }";
		}

		public String generateJsonRelationship(URI endNode, String relationshipType) {
			return "{ \"to\" : \""+endNode.toString()+"\", \"type\" : \""+relationshipType+"\" }";
		}

		public void setRelationshipProperties(URI relationshipUri, String name, Object value) throws URISyntaxException {
			URI propertyUri = new URI(extendNode(relationshipUri, "properties"));
			String entity = restUtil.json.toJsonNameValuePairCollection(name, value);
			WebResource resource = Client.create().resource(propertyUri);
			ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(entity).put(ClientResponse.class);
			NeoUtil.log.debug(String.format("PUT [%s] to [%s], status code [%d]", entity, propertyUri, response.getStatus()));
			response.close();
		}

		public void setRelationshipProperties(URI relationshipUri, Map<String, ?> properties) throws URISyntaxException {
			URI propertyUri = new URI(extendNode(relationshipUri, "properties"));
			String entity = restUtil.json.toJsonNameValuePairCollection(properties);
			WebResource resource = Client.create().resource(propertyUri);
			ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(entity).put(ClientResponse.class);
			NeoUtil.log.debug(String.format("PUT [%s] to [%s], status code [%d]", entity, propertyUri, response.getStatus()));
			response.close();
		}
		
		public void setRelationshipProperties(URI relationshipUri, String properties) throws URISyntaxException {
			URI propertyUri = new URI(extendNode(relationshipUri, "properties"));
			WebResource resource = Client.create().resource(propertyUri);
			ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(properties).put(ClientResponse.class);
			NeoUtil.log.debug(String.format("PUT [%s] to [%s], status code [%d]", properties, propertyUri, response.getStatus()));
			response.close();
		}
		
		public String getRelationshipsRaw(URI node) throws URISyntaxException {
			URI relationshipListURI =  new URI(extendNode(node, "relationships/all"));
			WebResource resource = Client.create().resource(relationshipListURI);
			ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).get(ClientResponse.class);
			NeoUtil.log.debug(String.format("PUT to [%s], status code [%d]", relationshipListURI, response.getStatus()));
			String entity = response.getEntity(String.class);
			response.close();
			return entity;
		}
		
		public JSONArray getRelationships(URI node) throws URISyntaxException {
			return (JSONArray) JSONValue.parse(getRelationshipsRaw(node));
		}

	}

	public class JSON {
		
		public RestAPI restUtil;
		
		public JSON(RestAPI restUtil) {
			this.restUtil = restUtil;
		}

		public List<JSONArray> getRowsFromCypherQuery(JSONObject object) {
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
		
		public List<JSONObject> getColumn(List<JSONArray> rows) {
			System.out.println(rows);
			return null;
		}

		public String toJsonNameValuePairCollection(String key, Object value) {
			if(value instanceof String) {
				return "{ \""+key+"\" : \""+value+"\" }";
			} else {
				return "{ \""+key+"\" : "+value+" }";
			}
		}

		public String toJsonNameValuePairCollection(Map<String, ?> properties) {
			StringBuilder builder = new StringBuilder();
			builder.append("{ ");
			for(Object key : properties.keySet()) {
				if(properties.get(key) instanceof String) {
					builder.append("\""+key+"\" : \""+properties.get(key)+"\", ");
				} else {
					builder.append("\""+key+"\" : "+properties.get(key)+", ");
				}
			}
			builder.delete(builder.length()-2, builder.length()-1);
			builder.append("}");
			return builder.toString();
		}

	}

}
