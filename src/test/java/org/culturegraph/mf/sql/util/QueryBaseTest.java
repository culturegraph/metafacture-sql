/*
 *  Copyright 2013 Deutsche Nationalbibliothek
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

import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.sql.SQLException;
import java.sql.Statement;

import org.culturegraph.mf.framework.StreamReceiver;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for class {@link QueryBase}.
 *
 * @author Christoph Böhme
 *
 */
public final class QueryBaseTest extends DatabaseBasedTest {

	private static final String CREATE_TABLE =
			"CREATE TABLE Test (key VARCHAR(10), name VARCHAR(50))";

	private static final String INSERT_NULL_VALUE =
			"INSERT INTO Test (key, name) VALUES ('1', null)";

	private static final String SELECT_ALL =
			"SELECT key, name FROM Test";

	private QueryBase sut;

	private Statement statement;

	@Mock
	private StreamReceiver receiver;

	@Before
	public void setup() throws SQLException {
		MockitoAnnotations.initMocks(this);
		sut = new QueryBase(getDatabase().getConnection(), QueryBase.DEFAULT_ID_COLUMN, true);
		statement = getDatabase().getConnection().createStatement();
	}

	@Before
	public void populateDatabase() throws SQLException {
		getDatabase()
			.run(CREATE_TABLE)
			.run(INSERT_NULL_VALUE);
	}

	@Test
	public void testShouldIgnoreNullValuesInColumns() throws SQLException {

		statement.execute(SELECT_ALL);
		sut.processResults(statement, receiver);

		final InOrder ordered = inOrder(receiver);
		ordered.verify(receiver).startRecord("");
		ordered.verify(receiver).literal(argThat(equalToIgnoringCase("key")), eq("1"));
		ordered.verify(receiver).endRecord();
		verifyNoMoreInteractions(receiver);
	}

}
