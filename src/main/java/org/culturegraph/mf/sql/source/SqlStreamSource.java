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
package org.culturegraph.mf.sql.source;

import java.sql.Connection;

import org.culturegraph.mf.framework.DefaultObjectPipe;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.culturegraph.mf.sql.util.JdbcUtil;
import org.culturegraph.mf.sql.util.PreparedQuery;
import org.culturegraph.mf.sql.util.QueryBase;

/**
 * Executes a prepared statement or stored procedure for each
 * object received. The string value of the object is passed
 * into the SQL statement as a parameter named ":obj". The
 * result sets created by executing the statement are returned
 * as records. For each row in the result set one record is
 * emitted.
 *
 * @author Christoph Böhme
 */
@Description("Executes a prepared statement or stored procedure for each object received.")
@In(String.class)
@Out(StreamReceiver.class)
public final class SqlStreamSource<T> extends
		DefaultObjectPipe<T, StreamReceiver> {

	public static final String PARAMETER = "obj";

	private final Connection connection;

	private String idColumnLabel = QueryBase.DEFAULT_ID_COLUMN;
	private String sql;

	private PreparedQuery statement;

	public SqlStreamSource(final String datasource) {
		this.connection = JdbcUtil.getConnection(datasource);
	}

	public SqlStreamSource(final Connection connection) {
		this.connection = connection;
	}

	public void setStatement(final String sql) {
		this.sql = sql;
	}

	public void setIdColumnLabel(final String idColumnLabel){
		this.idColumnLabel = idColumnLabel;
	}

	@Override
	public void process(final T obj) {
		if (statement == null) {
			statement = new PreparedQuery(connection, sql, idColumnLabel, true);
		}

		statement.clearParameters();
		statement.setParameter("obj", obj.toString());
		statement.execute(getReceiver());
	}

	@Override
	protected void onCloseStream() {
		if (statement != null) {
			statement.close();
		}
		JdbcUtil.closeConnection(connection);
	}

}
