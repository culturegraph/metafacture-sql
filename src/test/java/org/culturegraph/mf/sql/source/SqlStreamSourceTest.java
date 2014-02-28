package org.culturegraph.mf.sql.source;

import static org.junit.Assert.fail;

import java.sql.SQLException;

import org.culturegraph.mf.exceptions.FormatException;
import org.culturegraph.mf.sql.util.DatabaseBasedTest;
import org.culturegraph.mf.stream.sink.EventList;
import org.culturegraph.mf.stream.sink.StreamValidator;
import org.junit.Before;
import org.junit.Test;

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

	@Before
	public void populateDatabase() throws SQLException {
		getDatabase()
			.run(CREATE_TABLE)
			.run(String.format(INSERT, KEY1, NAME1))
			.run(String.format(INSERT, KEY2, NAME2));
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

		final SqlStreamSource<String> source = new SqlStreamSource<String>(getDatabase().getConnection());
		source.setStatement(SELECT);
		source.setIdColumnLabel(COLUMN1);
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

	@Test
	public void testShouldSetIdNameToColumnLabel() throws SQLException {
		final EventList expected = new EventList();
		expected.startRecord(KEY2);
		expected.literal(COLUMN1, KEY2);
		expected.literal(COLUMN2, NAME2);
		expected.endRecord();
		expected.closeStream();

		final SqlStreamSource<String> source = new SqlStreamSource<String>(getDatabase().getConnection());
		source.setStatement(SELECT_ALL);
		source.setIdColumnLabel(COLUMN1);
		final StreamValidator validator = new StreamValidator(expected.getEvents());
		source.setReceiver(validator);

		try {
			source.process(KEY2);
			source.closeStream();
		} catch (final FormatException e) {
			fail(e.toString());
		}
	}

}
