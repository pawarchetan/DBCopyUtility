package com.dbcopy.model;

import java.util.ArrayList;
import java.util.List;

public class Table {

	private String catalog;
	private String schema;
	private String name;
	private List<Field> fields = new ArrayList<>();
	
	public Table(String catalog, String schema, String table) {
		super();
		this.catalog = catalog;
		this.schema = schema;
		this.name = table;
	}

	public String getCatalog() {
		return catalog;
	}

	public String getSchema() {
		return schema;
	}

	public String getName() {
		return name;
	}

	public List<Field> getFields() {
		return fields;
	}

	public void setFields(List<Field> fields) {
		this.fields = fields;
	}
}
