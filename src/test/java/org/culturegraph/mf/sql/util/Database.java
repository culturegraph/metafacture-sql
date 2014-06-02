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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.dbutils.DbUtils;

/**
 * Convenience class for handling database connections
 * in test cases.
 *
 * @author Christoph Böhme
 *
 */
public final class Database {

	private final String url;

	private final Connection connection;
	private final Statement statement;

	public Database(final String url) throws SQLException {
		this.url = url;

		connection = getClosableConnection();
		try {
			statement = connection.createStatement();
		} catch (final SQLException e) {
			DbUtils.closeQuietly(connection);
			throw e;
		}
	}

	/**
	 * Returns a new connection to the database. The caller is
	 * responsible for closing the connection.
	 *
	 * @return a new connection to the database
	 * @throws SQLException
	 */
	public Connection getClosableConnection() throws SQLException {
		return DriverManager.getConnection(url);
	}

	/**
	 * Returns the default connection for the database. The caller
	 * must not close this connection. Use {@link getCloseableConnection}
	 * if you need a connection which can be closed.
	 *
	 * @return a connection to the database.
	 * @throws SQLException
	 */
	public Connection getConnection() throws SQLException {
		return connection;
	}

	public Database run(final String sql) throws SQLException {
		statement.execute(sql);
		return this;
	}

	public void close() {
		DbUtils.closeQuietly(statement);
		DbUtils.closeQuietly(connection);
	}

}
