package org.culturegraph.mf.sql.source;

import static org.junit.Assert.fail;

import java.sql.SQLException;

import org.culturegraph.mf.exceptions.FormatException;
import org.culturegraph.mf.sql.util.Database;
import org.culturegraph.mf.stream.sink.EventList;
import org.culturegraph.mf.stream.sink.StreamValidator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link SqlStreamSource}.
 *
 * @author Christoph Böhme
 *
 */
public final class SqlStreamSourceTest {

	private static final String DB_URL = "jdbc:h2:mem:";

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

	private Database database;

	@Before
	public void setupDBConnection() throws SQLException {
		database = new Database(DB_URL)
			.run(CREATE_TABLE)
			.run(String.format(INSERT, KEY1, NAME1))
			.run(String.format(INSERT, KEY2, NAME2));
	}

	@After
	public void closeDBConnection() throws SQLException {
		database.close();
	}

	@Test
	public void testSqlStreamSource() throws SQLException {
		final EventList expected = new EventList();
		expected.startRecord("");
		expected.literal(COLUMN2, NAME2);
		expected.endRecord();
		expected.startRecord("");
		expected.literal(COLUMN2, NAME1);
		expected.endRecord();
		expected.startRecord("");
		expected.literal(COLUMN2, NAME2);
		expected.endRecord();
		expected.closeStream();

		final SqlStreamSource<String> source = new SqlStreamSource<>(database.getConnection());
		source.setStatement(SELECT);
		final StreamValidator validator = new StreamValidator(expected.getEvents());
		source.setReceiver(validator);

		try {
			source.process(KEY2);
			source.process(KEY1);
			source.process(KEY2);
			source.closeStream();
		} catch (final FormatException e) {
			fail(e.toString());
		}
	}

}
