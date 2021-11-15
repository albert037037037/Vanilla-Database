/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
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
package org.vanilladb.core.query.algebra;

import java.util.Set;
import static org.vanilladb.core.sql.Type.VARCHAR;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;

/**
 * The {@link Plan} class corresponding to the <em>project</em> relational
 * algebra operator.
 */
public class ExplainPlan implements Plan {
	private Plan p;
	private Schema schema = new Schema();
    private Set<String> fldNames;

	/**
	 * Creates a new Explain node in the query tree, having the specified
	 * subquery and field list.
	 * 
	 * @param p
	 *            the subquery
	 */
	public ExplainPlan(Plan p) {
		this.p = p;
	}

	/**
	 * Creates a Explain scan for this query.
	 * 
	 * @see Plan#open()
	 */
	@Override
	public Scan open() {
		Scan s = p.open();
        schema.addField("query-plan", VARCHAR(500));
		return new ExplainScan(s, schema.fields(), getExplain(0));
	}

	/**
	 * Estimates the number of block accesses in the projection, which is the
	 * same as in the underlying query.
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		return p.blocksAccessed();
	}

	/**
	 * Returns the schema of the projection, which is taken from the field list.
	 * 
	 * @see Plan#schema()
	 */
	@Override
	public Schema schema() {
		return schema;
	}

	/**
	 * Returns the histogram that approximates the join distribution of the
	 * field values of query results.
	 * 
	 * @see Plan#histogram()
	 */
	@Override
	public Histogram histogram() {
		return p.histogram();
	}

	@Override
	public long recordsOutput() {
		return (long) p.recordsOutput();
	}

	@Override
	public String getExplain(int depth) {
		String str = "\n" + p.getExplain(depth) + String.format("Actual: #recs = %d", p.recordsOutput());
		return str;
	}
}
