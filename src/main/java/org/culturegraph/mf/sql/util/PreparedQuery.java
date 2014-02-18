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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.culturegraph.mf.exceptions.MetafactureException;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.sql.util.JdbcUtil.Bug;

/**
 * A class to simplify handling prepared statements.
 *
 * @author Christoph Böhme
 *
 */
public final class PreparedQuery extends QueryBase {

	private static final Pattern PARAMETER = Pattern.compile(":([a-zA-Z_]+[a-z-A-Z_0-9]*)");

	private final PreparedStatement statement;
	private final Map<String, List<Integer>> parameterMap = new HashMap<String, List<Integer>>();

	public PreparedQuery(final String datasource, final String sql, final boolean emitGeneratedKeys) {
		this(getConnection(datasource), sql, emitGeneratedKeys);
	}

	public PreparedQuery(final Connection connection, final String sql, final boolean emitGeneratedKeys) {
		super(connection, emitGeneratedKeys);

		final String mappedSql = initParameterMap(sql);

		final int autoGeneratedKeys;
		if (isEmitGeneratedKeys() &&
				!hasDriverBug(Bug.RETURN_GENERATED_KEYS_PRODUCES_INVALID_SQL)) {
			autoGeneratedKeys = Statement.RETURN_GENERATED_KEYS;
		} else {
			autoGeneratedKeys = Statement.NO_GENERATED_KEYS;
		}

		try {
			statement = connection.prepareStatement(mappedSql, autoGeneratedKeys);
		} catch (final SQLException e) {
			throw new MetafactureException(e);
		}
	}

	public void execute() {
		try { statement.execute(); }
		catch (final SQLException e) { throw new MetafactureException(e); }
	}

	public void execute(final StreamReceiver receiver) {
		try { statement.execute(); }
		catch (final SQLException e) { throw new MetafactureException(e); }

		processResults(statement, receiver);
	}

	public void close() {
		try {statement.close(); }
		catch (final SQLException e) { throw new MetafactureException(e); }
	}

	public void clearParameters() {
		try {
			statement.clearParameters();
		} catch (final SQLException e) {
			throw new MetafactureException(e);
		}
	}

	public void setParameter(final String name, final String value) {
		final List<Integer> paramPos = parameterMap.get(name);
		if (paramPos == null) {
			return;
		}
		try {
			for (final Integer pos : paramPos) {
				statement.setString(pos.intValue(), value);
			}
		} catch (final SQLException e) {
			throw new MetafactureException(e);
		}
	}

	private String initParameterMap(final String sql) {
		// TODO: Handle escape sequences. Exclude parameters within string literals
		parameterMap.clear();
		int pos = 1;
		final Matcher parameters = PARAMETER.matcher(sql);
		while (parameters.find()) {
			final String parameter = parameters.group(1);
			if (!parameterMap.containsKey(parameter)) {
				parameterMap.put(parameter, new LinkedList<Integer>());
			}
			parameterMap.get(parameter).add(Integer.valueOf(pos));
			pos += 1;
		}

		return parameters.replaceAll("?");
	}

}
