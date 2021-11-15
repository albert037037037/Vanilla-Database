/*******************************************************************************
 * Copyright 2016, 2017 elasql.org contributors
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

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasql.remote.groupcomm.client.DirectMessageListener;
import org.elasql.remote.groupcomm.client.GroupCommConnection;
import org.elasql.remote.groupcomm.client.GroupCommDriver;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.remote.SutDriver;

public class ElasqlBenchSpDriver implements SutDriver {
	
	private static final AtomicInteger NEXT_CONNECTION_ID = new AtomicInteger(0);
	
	private static GroupCommConnection conn = null;
	
	public ElasqlBenchSpDriver(int nodeId, DirectMessageListener messageListener) {
		if (conn == null) {
			GroupCommDriver driver = new GroupCommDriver(nodeId);
			conn = driver.init(messageListener);
		}
	}

	public SutConnection connectToSut() throws SQLException {
		try {
			// Each connection need a unique id
			return new ElasqlBenchSpConnection(conn, NEXT_CONNECTION_ID.getAndIncrement());
		} catch (Exception e) {
			e.printStackTrace();
			throw new SQLException(e);
		}
	}
}
