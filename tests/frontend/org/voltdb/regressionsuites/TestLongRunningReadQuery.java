/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.regressionsuites;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.BeforeClass;
import org.voltdb.BackendTarget;
import org.voltdb.LRRHelper;
import org.voltdb.ServerThread;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.AsyncCompilerAgent;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.utils.CSVLoader;

public class TestLongRunningReadQuery extends RegressionSuite {

    private int tableSize = 1000000;
    final String reportDir = "/home/user/workspace/voltdb";
    final String path_csv = String.format("%s/%s", reportDir, "mydata.csv");
    protected String userName = System.getProperty("user.name");

    private void checkProcStatistics(Client client)
            throws NoConnectionsException, IOException, ProcCallException {
        VoltTable vt;

        vt = client.callProcedure("@Statistics", "PROCEDUREPROFILE", 1).getResults()[0];
        //System.out.println(vt.toString());

    }
    
    private void loadTable() throws IOException, InterruptedException{
        String []myOptions = {
                "-f" + path_csv,
                "--port=21312",
                "--limitrows=" + tableSize,
                "R1"
        };
    	CSVLoader.testMode = true;
    	CSVLoader.main(myOptions);
    }

    private void fillTable(Client client) throws NoConnectionsException, IOException, ProcCallException {
        String sql;
        for (int i = 1; i <= tableSize; i++) {
            sql = "INSERT INTO R1 VALUES (" + i + ");";
            client.callProcedure("@AdHoc", sql);
        }
    }

    public void testLongRunningReadQuery() throws IOException, ProcCallException, InterruptedException {
         System.out.println("testLongRunningReadQuery...");
         
         loadTable();
         
         Client client = getClient();

         checkProcStatistics(client);

         subtest1Select(client);
         checkProcStatistics(client);
    }

    public void subtest1Select(Client client) throws IOException, ProcCallException {
        System.out.println("subtest1Select...");
        VoltTable files;
        VoltTable vt;
        String sql;

        sql = "SELECT * FROM R1;";
        files = client.callProcedure("@ReadOnlySlow", sql).getResults()[0];
        //System.out.println(files.toString());
        vt = LRRHelper.getTableFromFileTable(files);
        //System.out.println(vt.toString());
        assertEquals(tableSize,vt.getRowCount());
    }

    //
    // Suite builder boilerplate
    //

    public TestLongRunningReadQuery(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestLongRunningReadQuery.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE R1 ( " +
                " ID INT DEFAULT 0 NOT NULL"
                + ", COL1 INT "
                + ", COL2 INT "
                + ", COL3 INT "
                + ", COL4 INT "
                + ", COL5 INT "
                + ", COL6 INT "
                + ", COL7 INT "
                + ", COL8 INT "
                + ", COL9 INT "
                + ");"
                + ""
                ;
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }

        boolean success;

        config = new LocalCluster("longreads-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
