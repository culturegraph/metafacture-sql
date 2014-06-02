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

import org.culturegraph.mf.framework.DefaultObjectPipe;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.culturegraph.mf.sql.sink.SqlStatementSink;
import org.culturegraph.mf.sql.util.DirectQuery;
import org.culturegraph.mf.sql.util.JdbcUtil;
import org.culturegraph.mf.sql.util.QueryBase;

/**
 * Executes the received string object as an SQL statement.
 * Each row of the result sets produced by the statement is
 * emitted as a new record. The module also emits generated
 * keys as new records.
 *
 * Use {@code SqlStatementPipe} for SQL statements which do not
 * produce any result sets.
 *
 * In many situations it can be preferable to use prepared
 * statements instead of hand-crafted SQL. Use {@SqlStreamPipe}
 * in such situations.
 *
 * @see SqlStatementSink
 * @see SqlStreamPipe
 *
 * @author Christoph Böhme
 */
@Description("Executes the received string object as an SQL statement.")
@In(String.class)
@Out(StreamReceiver.class)
public final class SqlStatementPipe extends
		DefaultObjectPipe<String, StreamReceiver> {

	private final Connection connection;

	private String idColumnLabel = QueryBase.DEFAULT_ID_COLUMN;
	private DirectQuery query;

	public SqlStatementPipe(final String datasource) {
		this(JdbcUtil.getConnection(datasource));
	}

	public SqlStatementPipe(final Connection connection) {
		this.connection = connection;
	}

	public void setIdColumnLabel(final String idColumnLabel) {
		this.idColumnLabel = idColumnLabel;
	}

	@Override
	public void process(final String sql) {
		if (query == null) {
			query = new DirectQuery(connection, idColumnLabel, true);
		}

		query.execute(sql, getReceiver());
	}

	@Override
	protected void onCloseStream() {
		if (query != null) {
			query.close();
		}
		JdbcUtil.closeConnection(connection);
	}

}
