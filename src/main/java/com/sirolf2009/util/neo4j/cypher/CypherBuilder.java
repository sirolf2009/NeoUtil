package com.sirolf2009.util.neo4j.cypher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CypherBuilder {

	private static Logger log = Logger.getLogger("com.hardmatch.matcher.neo4j.cypher.CypherBuilder");

	private Match activeMatch;
	private List<IQuery> queries;

	public CypherBuilder() {
		queries = new ArrayList<IQuery>();
	}

	public Match match() {
		if(activeMatch != null) {
			log.log(Level.SEVERE, "Another match is already active. We'll close it, but result may not be as you had expected");
			activeMatch.end();
		}
		Match match = new Match(this);
		activeMatch = match;
		return match;
	}
	
	public String build(String returnValues) {
		String cypher="";
		for(IQuery match : queries) {
			cypher+=match.build();
		}
		cypher+="RETURN "+returnValues;
		return cypher;
	}
	
	public interface IQuery {
		public String build();
	}
	
	public class Match implements IQuery {

		private String varName;
		private String label;
		private String relatedTo;
		private String relatedType;
		private int relatedDirection;
		private Map<String, Object> props;
		private CypherBuilder builder;

		public Match(CypherBuilder builder) {
			props = new HashMap<String, Object>();
			varName = "";
			relatedTo = "";
			relatedType = "";
			this.builder = builder;
		}

		public String build() {
			String propsString = "";
			boolean firstRun = true;
			for(String key : props.keySet()) {
				propsString+=firstRun?"":" ,";
				firstRun = false;
				String value = props.get(key) instanceof String ? "\""+props.get(key).toString()+"\"" : props.get(key).toString();
				propsString+=key+":"+value;
			}

			return "MATCH ("+(varName.isEmpty()?"":(varName+":"))+label+" {"+propsString+"})"+relations()+"\n";
		}
		
		public String relations() {
			String relations = "";
			if(!relatedTo.isEmpty()) {
				if(relatedDirection == 0 || relatedDirection == 1) {
					relations+="-";
				} else {
					relations+="<";
				}
				if(!relatedType.isEmpty()) {
					relations+="[:"+relatedType+"]";
				} else {
					relations+="-";
				}
				if(relatedDirection == 2) {
					relations+="-";
				} else if(relatedDirection == 1) {
					relations+="->";
				}
				relations+="("+relatedTo+")";
			}
			return relations;
		}

		public Match byProperty(String key, Object value) {
			props.put(key, value);
			return this;
		}
		
		public Match store(String varName) {
			this.varName = varName;
			return this;
		}

		public Match byLabel(String label) {
			this.label = label;
			return this;
		}

		public Match relatedTo(String node) {
			relatedTo = node;
			relatedDirection = 0;
			return this;
		}

		public Match outgoingRelation(String node) {
			relatedTo = node;
			relatedDirection = 1;
			return this;
		}

		public Match ingoingRelation(String node) {
			relatedTo = node;
			relatedDirection = 2;
			return this;
		}
		
		public Match byRelationType(String node, String type) {
			relatedTo = node;
			relatedType = type;
			relatedDirection = 0;
			return this;
		}
		
		public Match outgoingRelationType(String node, String type) {
			relatedTo = node;
			relatedType = type;
			relatedDirection = 1;
			return this;
		}

		public Match ingoingRelationType(String node, String type) {
			relatedTo = node;
			relatedType = type;
			relatedDirection = 2;
			return this;
		}

		public CypherBuilder end() {
			queries.add(this);
			return builder;
		}
	}

}
