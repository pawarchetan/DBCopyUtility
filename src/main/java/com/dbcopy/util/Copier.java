package com.dbcopy.util;

import com.dbcopy.listener.CopierListener;
import com.dbcopy.model.Table;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public interface Copier {
	void copy();
	PreparedStatement setPreparedStatementParameters(PreparedStatement statement, Table table, ResultSet contents) throws Exception;
	Table getNextTable();
	void addCopierListener(CopierListener copierListener);
}