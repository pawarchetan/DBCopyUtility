package com.dbcopy.util;

import com.dbcopy.model.Table;

import java.util.Queue;

public class PooledCopier extends AbstractCopier {

	private final Queue<Table> tablePool;
	
	public PooledCopier(Database source, Database target, Queue<Table> tablePool) {
		super(source, target);
		this.tablePool = tablePool;
	}
	
	@Override
	public Table getNextTable() {
		Table table;
		synchronized (tablePool) {
			table = tablePool.poll();
		}
		return table;
	}

}
