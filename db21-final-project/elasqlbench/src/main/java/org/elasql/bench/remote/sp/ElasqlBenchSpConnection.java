/*******************************************************************************
 * Copyright 2016, 2018 elasql.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.elasql.bench.remote.sp;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

import org.elasql.remote.groupcomm.ElasqlSpResultSet;
import org.elasql.remote.groupcomm.client.GroupCommConnection;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.remote.SutResultSet;

public class ElasqlBenchSpConnection implements SutConnection {
	private GroupCommConnection conn;
	private int connectionId;

	public ElasqlBenchSpConnection(GroupCommConnection conn, int connId) {
		this.conn = conn;
		this.connectionId = connId;
	}

	@Override
	public SutResultSet callStoredProc(int pid, Object... pars)
			throws SQLException {
		ElasqlSpResultSet r = conn.callStoredProc(connectionId, pid, pars);
		return new ElasqlBenchSpResultSet(r);
	}

	@Override
	public Connection toJdbcConnection() {
		throw new RuntimeException("ElaSQL does not support JDBC.");
	}
	
	public void sendDirectMessage(int clientId, Serializable message) {
		conn.sendP2pMessageToClientNode(clientId, message);
	}
	
	public int getServerCount() {
		return conn.getServerCount();
	}
	
	public int getClientCount() {
		return conn.getClientCount();
	}
}

