package org.culturegraph.mf.sql.source;

import static org.mockito.Mockito.inOrder;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbutils.DbUtils;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.sql.util.DatabaseBasedTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/**
 * Tests for {@link SqlStreamSource}.
 *
 * @author Christoph Böhme
 *
 */
public final class SqlStreamSourceTest extends DatabaseBasedTest {

	private static final String COLUMN1 = "KEY";
	private static final String COLUMN2 = "NAME";
	private static final String KEY1 = "101";
	private static final String KEY2 = "102";
	private static final String NAME1 = "al-Chwarizmi";
	private static final String NAME2 = "Ibn an-Nadīm";

	private static final String CREATE_TABLE =
			"CREATE TABLE Test (key VARCHAR(10), name VARCHAR(50))";

	private static final String INSERT =
			"INSERT INTO Test (key, name) VALUES ('%s', '%s')";

	private static final String SELECT =
			"SELECT name FROM Test WHERE key = :obj";

	private static final String SELECT_ALL =
			"SELECT key, name FROM Test WHERE key = :obj";

	@Rule
	public MockitoRule mockitoRule = MockitoJUnit.rule();

	@Mock
	private StreamReceiver receiver;

	Connection connection;

	@Before
	public void populateDatabase() throws SQLException {
		connection = getDatabase().getClosableConnection();
		getDatabase()
			.run(CREATE_TABLE)
			.run(String.format(INSERT, KEY1, NAME1))
			.run(String.format(INSERT, KEY2, NAME2));
	}

	@After
	public void ensureConnectionClosed() {
		DbUtils.closeQuietly(connection);
	}

	@Test
	public void shouldQueryAndReturnRecords() throws SQLException {
		final SqlStreamSource<String> source = new SqlStreamSource<>(connection);
		source.setStatement(SELECT);
		source.setIdColumnLabel(COLUMN1);
		source.setReceiver(receiver);

		source.process(KEY2);
		source.process(KEY1);
		source.process(KEY2);
		source.closeStream();

		final InOrder ordered = inOrder(receiver);
		ordered.verify(receiver).startRecord("");
		ordered.verify(receiver).literal(COLUMN2, NAME2);
		ordered.verify(receiver).endRecord();
		ordered.verify(receiver).startRecord("");
		ordered.verify(receiver).literal(COLUMN2, NAME1);
		ordered.verify(receiver).endRecord();
		ordered.verify(receiver).startRecord("");
		ordered.verify(receiver).literal(COLUMN2, NAME2);
		ordered.verify(receiver).endRecord();
	}

	@Test
	public void shouldSetIdNameToColumnLabel() throws SQLException {
		final SqlStreamSource<String> source = new SqlStreamSource<>(connection);
		source.setStatement(SELECT_ALL);
		source.setIdColumnLabel(COLUMN1);
		source.setReceiver(receiver);

		source.process(KEY2);
		source.closeStream();

		final InOrder ordered = inOrder(receiver);
		ordered.verify(receiver).startRecord(KEY2);
		ordered.verify(receiver).literal(COLUMN1, KEY2);
		ordered.verify(receiver).literal(COLUMN2, NAME2);
		ordered.verify(receiver).endRecord();
	}

}
