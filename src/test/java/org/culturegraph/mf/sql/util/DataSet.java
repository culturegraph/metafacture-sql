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

import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;

/**
 * Convenience class for comparing JDBC result sets in assertions.
 *
 * @author Christoph Böhme
 *
 */
public final class DataSet {

	final List<Map<String, Object>> data;

	public DataSet() {
		data = new LinkedList<Map<String, Object>>();
	}

	public DataSet(final Database database, final String query) throws SQLException {
		data = new QueryRunner().query(database.getConnection(), query, new MapListHandler());
	}

	public DataSet addRow() {
		data.add(new HashMap<String, Object>());
		return this;
	}

	public DataSet put(final String column, final String value) {
		data.get(data.size() - 1).put(column, value);
		return this;
	}

	@Override
	public int hashCode() {
		return data.hashCode();
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final DataSet other = (DataSet) obj;
		return data.equals(other.data);
	}

}
