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

import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.culturegraph.mf.framework.helpers.DefaultObjectPipe;
import org.culturegraph.mf.sql.sink.SqlStatementSink;
import org.culturegraph.mf.sql.util.DirectQuery;
import org.culturegraph.mf.sql.util.JdbcUtil;

/**
 * Executes the received string object as an SQL statement. Each row of the
 * result sets produced by the statement is emitted as a new record. The module
 * also emits generated keys as new records.
 * <p>
 * Use {@code SqlStatementPipe} for SQL statements which do not produce any
 * result sets.
 * <p>
 * In many situations it can be preferable to use prepared statements instead of
 * hand-crafted SQL. Use {@link SqlStreamPipe} in such situations.
 *
 * @author Christoph Böhme
 * @see SqlStatementSink
 * @see SqlStreamPipe
 */
@Description("Executes the received string object as an SQL statement.")
@In(String.class)
@Out(StreamReceiver.class)
public final class SqlStatementPipe extends
		DefaultObjectPipe<String, StreamReceiver> {

	private final Connection connection;

	private String idColumnLabel = DirectQuery.DEFAULT_ID_COLUMN;
	private DirectQuery query;

	public SqlStatementPipe(final String dataSource) {
		this(JdbcUtil.getConnection(dataSource));
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
