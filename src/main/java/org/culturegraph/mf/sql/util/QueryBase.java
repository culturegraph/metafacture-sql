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

import org.culturegraph.mf.exceptions.MetafactureException;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.sql.util.JdbcUtil.Bug;

/**
 * Base class for prepared statements and individual statements.
 *
 * @author Christoph Böhme
 *
 */
public class QueryBase {

	public static final String DEFAULT_ID_COLUMN = "_id";

	private final String idColumnLabel;
	private final boolean emitGeneratedKeys;
	private final EnumSet<Bug> driverBugs;

	public QueryBase(final Connection connection, final String idColumnLabel,
			final boolean emitGeneratedKeys) {
		this.idColumnLabel = idColumnLabel;
		this.emitGeneratedKeys = emitGeneratedKeys;
		driverBugs = JdbcUtil.getDriverBugs(connection);
	}

	protected final boolean hasDriverBug(final Bug bug) {
		return driverBugs.contains(bug);
	}

	protected final boolean isEmitGeneratedKeys() {
		return emitGeneratedKeys;
	}

	protected void processResults(final Statement statement,
			final StreamReceiver receiver) {
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
			try {
				if (resultSet != null) {
					emitRecords(resultSet, receiver);
				}
				if (isEmitGeneratedKeys()) {
					final ResultSet generatedKeys = statement.getGeneratedKeys();
					try {
						emitRecords(generatedKeys, receiver);
					} finally {
						try { generatedKeys.close(); }
						catch (final SQLException e) { /* Ignore exception */ }
					}
				}
			} finally {
				if (resultSet != null) {
					try { resultSet.close(); }
					catch (final SQLException e) { /* Ignore exception */ }
				}
			}
		} catch (final SQLException e) {
			throw new MetafactureException(e);
		}
	}

	private void emitRecords(final ResultSet resultSet,
			final StreamReceiver receiver) {
		try {
			boolean hasIdColumnLabel = false;
			final ResultSetMetaData resultSetMeta = resultSet.getMetaData();
			for (int i = 1; i <= resultSetMeta.getColumnCount(); ++i) {
				if (idColumnLabel.equalsIgnoreCase(resultSetMeta.getColumnLabel(i))) {
					hasIdColumnLabel = true;
					break;
				}
			}

			while (resultSet.next()) {
				if (hasIdColumnLabel) {
					receiver.startRecord(resultSet.getString(idColumnLabel));
				} else {
					receiver.startRecord("");
				}
				for (int i = 1; i <= resultSetMeta.getColumnCount(); ++i) {
					final String value = resultSet.getString(i);
					if (value != null) {
						receiver.literal(resultSetMeta.getColumnLabel(i), value);
					}
				}
				receiver.endRecord();
			}
		} catch (final SQLException e) {
			throw new MetafactureException(e);
		}
	}

}