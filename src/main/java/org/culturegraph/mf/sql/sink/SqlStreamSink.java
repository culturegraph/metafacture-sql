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

import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.helpers.DefaultStreamReceiver;
import org.culturegraph.mf.sql.pipe.SqlStreamPipe;
import org.culturegraph.mf.sql.util.JdbcUtil;
import org.culturegraph.mf.sql.util.PreparedQuery;

/**
 * Executes a prepared query for each record received. The
 * prepared query supports named parameters (written as
 * :PARAMETER). If a literal name matches a parameter name
 * its value is used for the parameter. Literals in entities
 * are not prefixed with entity name.
 *
 * {@code SqlStreamSinkTest} does not evaluate the result set
 * which may be returned by executing the query. This
 * makes this module suitable for performing operations such
 * as INSERT, UPDATE or DELETE.
 *
 * Use {@code SqlStreamPipe} if access to the results of the
 * SQL query is required.
 *
 * @see SqlStreamPipe
 *
 * @author Christoph Böhme
 *
 */
@Description("Executes a prepared query for each record received.")
@In(StreamReceiver.class)
public final class SqlStreamSink extends DefaultStreamReceiver {

	public static final String ID_PARAMETER = "_ID";

	private final Connection connection;

	private PreparedQuery query;

	public SqlStreamSink(final String datasource) {
		this.connection = JdbcUtil.getConnection(datasource);
	}

	public SqlStreamSink(final Connection connection) {
		this.connection = connection;
	}

	public void setQuery(final String sql) {
		this.query = new PreparedQuery(connection, sql, false);
	}

	@Override
	public void startRecord(final String id) {
		query.clearParameters();
		query.setParameter(ID_PARAMETER, id);
	}

	@Override
	public void endRecord() {
		query.execute();
	}

	@Override
	public void literal(final String name, final String value) {
		query.setParameter(name, value);
	}

	@Override
	public void closeStream() {
		query.close();
		JdbcUtil.closeConnection(connection);
	}

}
