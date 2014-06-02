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

import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;

/**
 * Base class for test case which require a database connection.
 *
 * @author Christoph Böhme
 *
 */
public class DatabaseBasedTest {

	private static final String DB_URL = "jdbc:h2:mem:sql-metafacture-test";

	private Database database;

	protected Database getDatabase() {
		return database;
	}

	@Before
	public void setupDBConnection() throws SQLException {
		database = new Database(DB_URL);
	}

	@After
	public void closeDBConnection() throws SQLException {
		database.close();
	}

}
