package com.dbcopy.util;

import com.dbcopy.model.Table;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

public interface Database {
	void connect() throws Exception;
	List<Table> getTables(List<String> excludes);
	int countContentsForTable(Table table);
	ResultSet getContentsForTable(Table table) throws Exception;
	String buildTableName(Table table);
	PreparedStatement buildPreparedInsertStatement(Table sourceTable, Table targetTable) throws Exception;
	Table createTargetTable(Table table, String catalog);
}
