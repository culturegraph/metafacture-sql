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
package org.culturegraph.mf.sql.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.EnumSet;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.culturegraph.mf.exceptions.MetafactureException;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.sql.util.JdbcUtil.Bug;

/**
 * Base class for prepared statements and individual statements.
 *
 * @author Christoph Böhme
 *
 */
public abstract class QueryBase {

	private final EnumSet<Bug> driverBugs;
	private final boolean emitGeneratedKeys;

	public QueryBase(final Connection connection, final boolean emitGeneratedKeys) {
		this.emitGeneratedKeys = emitGeneratedKeys;
		driverBugs = JdbcUtil.getDriverBugs(connection);
	}

	protected final boolean hasDriverBug(final Bug bug) {
		return driverBugs.contains(bug);
	}

	protected final boolean isEmitGeneratedKeys() {
		return emitGeneratedKeys;
	}

	protected void processResults(final Statement statement, final StreamReceiver receiver) {
		try {
			ResultSet resultSet;
			if (hasDriverBug(Bug.GET_RESULT_SET_THROWS_ILLEGAL_EXCEPTION)) {
				try {
					resultSet = statement.getResultSet();
				} catch (final SQLException e) {
					if (statement.getUpdateCount() == -1) {
						throw e;
					}
					resultSet = null;
				}
			} else {
				resultSet = statement.getResultSet();
			}
			if (resultSet != null) {
				emitRecords(resultSet, receiver);
			}
			if (isEmitGeneratedKeys()) {
				emitRecords(statement.getGeneratedKeys(), receiver);
			}
		} catch (final SQLException e) {
			throw new MetafactureException(e);
		}

	}

	protected static Connection getConnection(final String datasourceName) {
		try {
			final InitialContext ctx = new InitialContext();
			final DataSource datasource = (DataSource) ctx.lookup(datasourceName);
			return datasource.getConnection();
		} catch (final NamingException | SQLException e) {
			throw new MetafactureException(e);
		}
	}

	private static void emitRecords(final ResultSet resultSet, final StreamReceiver receiver) {
		try {
			final ResultSetMetaData resultSetMeta = resultSet.getMetaData();
			while (resultSet.next()) {
				receiver.startRecord("");  // TODO: Add id
				for (int i = 1; i <= resultSetMeta.getColumnCount(); ++i) {
					receiver.literal(resultSetMeta.getColumnName(i), resultSet.getString(i));
				}
				receiver.endRecord();
			}
		} catch (final SQLException e) {
			throw new MetafactureException(e);
		} finally {
			try { resultSet.close(); }
			catch (final SQLException e) { /* Ignore exception */ }
		}
	}

}