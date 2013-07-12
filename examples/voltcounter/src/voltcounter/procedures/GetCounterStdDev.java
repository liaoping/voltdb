/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
//
// Get Counter Standard Dev Value provided a counter_id and counter_class_id
//
package voltcounter.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

@ProcInfo(
        partitionInfo = "counter_rollups.counter_class_id:0",
        singlePartition = true)
public class GetCounterStdDev extends VoltProcedure {

    /**
     * Get AVG
     */
    public final SQLStmt selectAvgStmt = new SQLStmt("SELECT AVG(rollup_value) "
            + "FROM counter_rollups "
            + "WHERE counter_class_id = ? "
            + "AND counter_id = ?");
    /**
     * Get Rollup Value
     */
    public final SQLStmt selectStmt = new SQLStmt("SELECT rollup_value "
            + "FROM counter_rollups "
            + "WHERE counter_class_id = ? "
            + "AND counter_id = ?");

    /**
     *
     * @param srollup_id
     * @return
     */
    public VoltTable run(long counter_class_id, long counter_id) {

        // get rollup values
        voltQueueSQL(selectAvgStmt, counter_class_id, counter_id);
        voltQueueSQL(selectStmt, counter_class_id, counter_id);
        VoltTable retresult = new VoltTable(
                new VoltTable.ColumnInfo("sigma", VoltType.BIGINT));

        VoltTable results[] = voltExecuteSQL();
        if (results[0].getRowCount() != 1) {
            retresult.addRow(new Object[]{0});
            return retresult;
        }
        results[0].advanceRow();
        double avg = results[0].getLong(0);
        if (results[1].getRowCount() <= 0) {
            retresult.addRow(new Object[]{0});
            return retresult;
        }
        VoltTable result = results[1];
        double sqtotal = 0.0;
        for (int i = 0; i < result.getRowCount(); i++) {
            if (result.advanceRow()) {
                double val = (double) result.getLong(0);
                double sqval = Math.pow((val - avg), 2);
                sqtotal += sqval;
            }
        }
        double stddev = Math.sqrt(sqtotal / result.getRowCount());

        retresult.addRow(new Object[]{stddev});
        return retresult;
    }
}
