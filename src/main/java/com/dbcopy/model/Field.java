package com.dbcopy.model;

public class Field {

	private String name;
	private String type;
	private int intType;

	public Field(String name, String type, int intType) {
		this.name = name;
		this.type = type;
		this.intType = intType;
	}

	public String getType() {
		return type;
	}

	public String getName() {
		return name;
	}

	public int getIntType() {
		return intType;
	}
}
