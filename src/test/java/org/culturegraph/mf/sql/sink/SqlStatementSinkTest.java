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

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;

import org.culturegraph.mf.sql.util.DataSet;
import org.culturegraph.mf.sql.util.DatabaseBasedTest;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link SQLStatementSink}.
 *
 * @author Christoph Böhme
 *
 */
public final class SqlStatementSinkTest extends DatabaseBasedTest {

	private static final String COLUMN1 = "key";
	private static final String COLUMN2 = "name";
	private static final String KEY1 = "101";
	private static final String KEY2 = "102";
	private static final String NAME1 = "al-Chwarizmi";
	private static final String NAME2 = "Ibn an-Nadīm";

	private static final String CREATE_TABLE =
			"CREATE TABLE Test (key VARCHAR(10), name VARCHAR(50))";

	private static final String INSERT =
			"INSERT INTO Test (key, name) VALUES ('%s', '%s')";

	private static final String SELECT =
			"SELECT * FROM Test";

	@Before
	public void populateDatabase() throws SQLException {
		getDatabase()
			.run(CREATE_TABLE);
	}

	@Test
	public void testSqlStatementSink() throws SQLException {
		final SqlStatementSink sink = new SqlStatementSink(getDatabase().getClosableConnection());
		sink.process(String.format(INSERT, KEY1, NAME1));
		sink.process(String.format(INSERT, KEY2, NAME2));
		sink.closeStream();

		final DataSet actual = new DataSet(getDatabase(), SELECT);
		final DataSet expected = new DataSet()
			.addRow()
				.put(COLUMN1, KEY1)
				.put(COLUMN2, NAME1)
			.addRow()
				.put(COLUMN1, KEY2)
				.put(COLUMN2, NAME2);

		assertEquals(expected, actual);
	}

}
