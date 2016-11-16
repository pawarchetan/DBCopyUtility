package com.dbcopy.util;

public class CopierTask implements Runnable {

	private Copier copier;
	
	public CopierTask(Copier copier) {
		this.copier = copier;
	}
	
	@Override
	public void run() {
		copier.copy();
	}

}
