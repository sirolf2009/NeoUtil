package com.sirolf2009.util.neo4j.label;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

public class LabelSimple implements Label, RelationshipType {

	private String name;
	
	public LabelSimple(String name) {
		this.name = name;
	}

	@Override
	public String name() {
		return name;
	}

}
