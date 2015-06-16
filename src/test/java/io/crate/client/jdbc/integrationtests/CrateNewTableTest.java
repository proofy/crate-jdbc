/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.client.jdbc.integrationtests;

import io.crate.client.CrateTestServer;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Locale;

import static org.hamcrest.core.Is.*;
import static org.junit.Assert.assertThat;

public class CrateNewTableTest {

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@ClassRule
	public static CrateTestServer testServer = new CrateTestServer("tabletest");

	public String hostAndPort = String.format(Locale.ENGLISH, "%s:%d",
			testServer.crateHost, testServer.transportPort);

	public Connection c1;

	public String schemaName = "my_schema";
	public String tableName = "my_table";

	@Before
	public void initDriverRegistration() throws Exception {
		Class.forName("io.crate.client.jdbc.CrateDriver");

		c1 = DriverManager.getConnection("jdbc:crate://" + hostAndPort + '/'
				+ schemaName);

	}

	@Test
	public void testCreateTableWithoutSchemaName() throws Exception {

		PreparedStatement pstmt = c1
				.prepareStatement("create table " + tableName
						+ " ( first_column integer, second_column string )");
		assertThat(pstmt.execute(), is(false));
		pstmt = c1.prepareStatement("select schema_name, table_name from information_schema.tables where table_name='"
				+ tableName + "'");
		assertThat(pstmt.execute(), is(true)); // there should be a return value
		ResultSet rSet = pstmt.getResultSet();
		assertThat(rSet.next(), is(true)); // there should be a result
		assertThat(rSet.getString(1), is(schemaName));
		assertThat(rSet.getString(2), is(tableName));
		pstmt = c1.prepareStatement("insert into " + tableName + "(first_column, second_column) values ( 42, 'testing')");
		assertThat(pstmt.execute(), is(false));
		pstmt = c1.prepareStatement("select * from " + tableName );
		assertThat(pstmt.execute(), is(true)); // there should be a return value
		ResultSet rSet2 = pstmt.getResultSet();
		assertThat(rSet2.next(), is(true)); // there should be a result
		assertThat(rSet2.getInt(1), is(42));
		assertThat(rSet2.getString(2), is("testing"));

	}

}
