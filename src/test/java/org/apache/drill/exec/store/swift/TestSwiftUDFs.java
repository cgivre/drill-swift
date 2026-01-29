/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.store.swift;


import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSwiftUDFs extends ClusterTest {


    public static final String MESSAGE_1 = "{1:F01BICFOOYYAXXX8683497519}{2:O1031535051028ESPBESMMAXXX54237522470510281535N}{3:{113:ROMF}{108:0510280182794665}{119:STP}}{4:\n" +
            ":20:0061350113089908\n" +
            ":13C:/RNCTIME/1534+0000\n" +
            ":23B:CRED\n" +
            ":23E:SDVA\n" +
            ":32A:061028EUR100000,\n" +
            ":33B:EUR100000,\n" +
            ":50K:/12345678\n" +
            "AGENTES DE BOLSA FOO AGENCIA\n" +
            "AV XXXXX 123 BIS 9 PL\n" +
            "12345 BARCELONA\n" +
            ":52A:/2337\n" +
            "FOOAESMMXXX\n" +
            ":53A:FOOAESMMXXX\n" +
            ":57A:BICFOOYYXXX\n" +
            ":59:/ES0123456789012345671234\n" +
            "FOO AGENTES DE BOLSA ASOC\n" +
            ":71A:OUR\n" +
            ":72:/BNF/TRANSF. BCO. FOO\n" +
            "-}{5:{MAC:88B4F929}{CHK:22EF370A4073}}";

    @BeforeClass
    public static void setup() throws Exception {
        ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
    }


    @Test
    public void testSwiftParserUDF() throws Exception {
        String sql = "select parse_swift('"+ MESSAGE_1 + "') as swift from (values(1))";
        RowSet results = client.queryBuilder().sql(sql).rowSet();
        results.print();
        results.clear();
    }
}
