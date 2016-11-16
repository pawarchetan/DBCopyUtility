package com.dbcopy.util;

import com.dbcopy.model.Table;

import java.util.Iterator;
import java.util.List;

public class TableListCopier extends AbstractCopier implements Copier {
	
	private Iterator<Table> tableIterator;
	
	public TableListCopier(Database source, Database target, List<Table> tablesToCopy) {
		super(source, target);
		this.tableIterator = tablesToCopy.iterator();
	}
	
	@Override
	public Table getNextTable() {
		Table table = null;
		if(tableIterator.hasNext()) table = tableIterator.next();
		return table;
	}
	
}