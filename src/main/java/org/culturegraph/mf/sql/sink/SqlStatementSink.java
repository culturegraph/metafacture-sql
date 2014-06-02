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
package org.culturegraph.mf.sql.sink;

import java.sql.Connection;

import org.culturegraph.mf.framework.DefaultObjectReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.sql.pipe.SqlStatementPipe;
import org.culturegraph.mf.sql.util.DirectQuery;
import org.culturegraph.mf.sql.util.JdbcUtil;

/**
 * Executes the received string object as an SQL statement. Any
 * result sets which may be produced by executing the statement
 * are discarded.
 *
 * Use {@code SqlStatementPipe} if access to the result sets is
 * required.
 *
 * In many situations it can be preferable to use prepared
 * statements instead of raw SQL statements. Use {@SqlStreamSink}
 * in such situations.
 *
 * @see SqlStatementPipe
 * @see SqlStreamSink
 *
 * @author Christoph Böhme
 */
@Description("Executes the received string object as an SQL statement.")
@In(String.class)
public final class SqlStatementSink extends DefaultObjectReceiver<String> {

	private final Connection connection;

	private final DirectQuery query;

	public SqlStatementSink(final String datasource) {
		this(JdbcUtil.getConnection(datasource));
	}

	public SqlStatementSink(final Connection connection) {
		this.connection = connection;
		query = new DirectQuery(connection, false);
	}

	@Override
	public void process(final String sql) {
		query.execute(sql);
	}

	@Override
	public void closeStream() {
		query.close();
		JdbcUtil.closeConnection(connection);
	}

}
