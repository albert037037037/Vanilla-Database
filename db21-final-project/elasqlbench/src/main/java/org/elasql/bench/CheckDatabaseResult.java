package org.elasql.bench;

import java.io.Serializable;

public class CheckDatabaseResult implements Serializable {
	
	private static final long serialVersionUID = 20200213001L;
	
	private boolean result;
	
	public CheckDatabaseResult(boolean result) {
		this.result = result;
	}
	
	public boolean getResult() {
		return result;
	}
}
