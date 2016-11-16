package com.dbcopy.listener;

import com.dbcopy.model.Table;

public interface CopierListener {
	void startCopyTable(Table table, long totalRows);
	void copyTableStatus(Table table, long currentPos, long totalRows);
	void error(Table table, Exception exception);
	void endCopyTable(Table table);
	void startCopy();
}
