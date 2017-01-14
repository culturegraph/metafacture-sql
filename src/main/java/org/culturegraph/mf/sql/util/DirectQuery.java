package org.culturegraph.mf.sql.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.culturegraph.mf.framework.MetafactureException;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.sql.util.JdbcUtil.Bug;

public final class DirectQuery extends QueryBase {

	private final Statement statement;

	public DirectQuery(final Connection connection, final boolean emitGeneratedKeys) {
		this(connection, QueryBase.DEFAULT_ID_COLUMN, emitGeneratedKeys);
	}

	public DirectQuery(final Connection connection, final String idColumnLabel, final boolean emitGeneratedKeys) {
		super(connection, idColumnLabel, emitGeneratedKeys);

		try {
			statement = connection.createStatement();
			if (hasDriverBug(Bug.RESULT_SET_STREAMING_ONLY_IF_FETCH_SIZE_IS_MIN_VALUE)) {
				statement.setFetchSize(Integer.MIN_VALUE);
			}
		} catch (final SQLException e) {
			throw new MetafactureException(e);
		}
	}

	public void execute(final String sql) {
		try { statement.execute(sql, Statement.NO_GENERATED_KEYS); }
		catch (final SQLException e) { throw new MetafactureException(e); }
	}

	public void execute(final String sql, final StreamReceiver receiver) {
		final int autoGeneratedKeys;
		if (isEmitGeneratedKeys() &&
				!hasDriverBug(Bug.RETURN_GENERATED_KEYS_PRODUCES_INVALID_SQL)) {
			autoGeneratedKeys = Statement.RETURN_GENERATED_KEYS;
		} else {
			autoGeneratedKeys = Statement.NO_GENERATED_KEYS;
		}

		try { statement.execute(sql, autoGeneratedKeys); }
		catch (final SQLException e) { throw new MetafactureException(e); }

		processResults(statement, receiver);
	}

	public void close() {
		try {statement.close(); }
		catch (final SQLException e) { throw new MetafactureException(e); }
	}

}
