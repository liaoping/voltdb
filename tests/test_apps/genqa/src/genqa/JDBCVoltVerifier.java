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
package genqa;

import genqa.VerifierUtils.Config;

import java.io.IOException;
import java.sql.Connection;
import java.util.Timer;

import org.voltcore.logging.VoltLogger;
import org.voltdb.CLIConfig;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.NoConnectionsException;

public class JDBCVoltVerifier {
    // Volt DB client handle
    static Client client;

    // JDBC client handle
    Connection conn;

    static Config config;

    // validated command line configuration
    // static Config config;
    // Timer for periodic stats printing
    static Timer statsTimer;
    static Timer checkTimer;
    // Benchmark start time
    long benchmarkStartTS;


    public static void main(String[] args) {
        Client client;
        Connection jdbcConnection;
        int ratelimit = Integer.MAX_VALUE;

        // setup configuration from command line arguments and defaults
        Config config = new VerifierUtils.Config();
        config.parse(JDBCVoltVerifier.class.getName(), args);
        System.out.println("Configuration settings:");
        System.out.println(config.getConfigDumpString());

        System.out.println("Connecting to " + config.servers);
        try {
            client = VerifierUtils.dbconnect(config.servers, ratelimit);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        System.out.println("Connecting to the JDBC target (Vertica?)");
        jdbcConnection = JDBCGetData.jdbcConnect(config);
    }
}
