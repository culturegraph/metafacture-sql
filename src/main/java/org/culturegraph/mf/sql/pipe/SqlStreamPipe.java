/*
 *  Copyright 2013 Christoph Böhme
 *
 *  Licensed under the Apache License, Version 2.0 the "License";
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.culturegraph.mf.sql.pipe;

import java.sql.Connection;

import org.culturegraph.mf.framework.DefaultStreamPipe;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.sql.sink.SqlStreamSink;
import org.culturegraph.mf.sql.util.PreparedQuery;

/**
 * Executes a prepared query for each record received. Each
 * row of the result sets produced by the query is emitted
 * as a new record. The module also emits generated keys as new
 * records.
 *
 * Use {@code SqlStreamSink} for SQL statements which do not
 * produce any result sets.
 *
 * @see SqlStreamSink

 * @author Christoph Böhme
 */
public final class SqlStreamPipe extends DefaultStreamPipe<StreamReceiver> {

	public static final String ID_PARAMETER = "_ID";

	private final String datasource;
	private final Connection connection;

	private PreparedQuery query;

	public SqlStreamPipe(final String datasource) {
		this.datasource = datasource;
		this.connection = null;
	}

	public SqlStreamPipe(final Connection connection) {
		this.datasource = null;
		this.connection = connection;
	}

	public void setQuery(final String sql) {
		if (datasource != null) {
			this.query = new PreparedQuery(datasource, sql, true);
		} else if (connection != null) {
			this.query = new PreparedQuery(connection, sql, true);
		}
	}

	@Override
	public void startRecord(final String id) {
		query.clearParameters();
		query.setParameter(ID_PARAMETER, id);
	}

	@Override
	public void endRecord() {
		query.execute(getReceiver());
	}

	@Override
	public void literal(final String name, final String value) {
		query.setParameter(name, value);
	}

	@Override
	protected void onCloseStream() {
		query.close();
	}

}
