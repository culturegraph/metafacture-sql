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
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.culturegraph.mf.sql.sink.SqlStreamSink;
import org.culturegraph.mf.sql.util.JdbcUtil;
import org.culturegraph.mf.sql.util.PreparedQuery;
import org.culturegraph.mf.sql.util.QueryBase;

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
@Description("Executes a prepared query for each record received.")
@In(StreamReceiver.class)
@Out(StreamReceiver.class)
public final class SqlStreamPipe extends DefaultStreamPipe<StreamReceiver> {

	public static final String ID_PARAMETER = "_ID";

	private final Connection connection;

	private String idColumnLabel = QueryBase.DEFAULT_ID_COLUMN;
	private String sql;

	private PreparedQuery query;

	public SqlStreamPipe(final String datasource) {
		this.connection = JdbcUtil.getConnection(datasource);
	}

	public SqlStreamPipe(final Connection connection) {
		this.connection = connection;
	}

	public void setQuery(final String sql) {
		this.sql = sql;
	}

	public void setIdColumnLabel(final String idColumnLabel){
		this.idColumnLabel = idColumnLabel;
	}

	@Override
	public void startRecord(final String id) {
		if (query == null) {
			this.query = new PreparedQuery(connection, sql, idColumnLabel, true);
		}

		query.clearParameters();
		query.setParameter(ID_PARAMETER, id);
	}

	@Override
	public void endRecord() {
		assert query != null: "startRecord was not called";

		query.execute(getReceiver());
	}

	@Override
	public void literal(final String name, final String value) {
		assert query != null: "startRecord was not called";

		query.setParameter(name, value);
	}

	@Override
	protected void onCloseStream() {
		if (query != null) {
			query.close();
		}
		JdbcUtil.closeConnection(connection);
	}

}
