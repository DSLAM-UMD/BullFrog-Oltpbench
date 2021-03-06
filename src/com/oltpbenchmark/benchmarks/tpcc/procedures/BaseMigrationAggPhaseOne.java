/******************************************************************************
 *  Copyright 2020 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.benchmarks.tpcc.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;
import java.text.MessageFormat;

import java.util.concurrent.atomic.*;

import org.apache.log4j.Logger;

import com.oltpbenchmark.DBWorkload;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;

public class BaseMigrationAggPhaseOne extends TPCCProcedure {

    private static final Logger LOG = Logger.getLogger(BaseMigrationAggPhaseOne.class);
    private static AtomicLong numRun = new AtomicLong(0);

    public SQLStmt migrationSQL = new SQLStmt(
        "insert into orderline_agg(" +
        " ol_amount_sum, ol_quantity_avg, ol_o_id, ol_d_id, ol_w_id) " +
        " (select " +
        "  sum(ol_amount), avg(ol_quantity), ol_o_id, ol_d_id, ol_w_id " +
        "  from order_line " +
        "  group by ol_o_id, ol_d_id, ol_w_id);");

        // insert into orderline_agg(
        //     ol_amount_sum, ol_quantity_avg, ol_o_id, ol_d_id, ol_w_id)
        // (select
        //     sum(ol_amount), avg(ol_quantity), ol_o_id, ol_d_id, ol_w_id
        // from order_line
        // group by ol_o_id, ol_d_id, ol_w_id) on conflict (ol_o_id, ol_d_id, ol_w_id) do nothing;

    private PreparedStatement migration = null;

    public ResultSet run(Connection conn, Random gen,
            int w_id, int numWarehouses,
            int terminalDistrictLowerID, int terminalDistrictUpperID,
            TPCCWorker w) throws SQLException {

        boolean trace = LOG.isTraceEnabled();

        migration = this.getPreparedStatement(conn, migrationSQL);

        if (numRun.getAndIncrement() == 0) {
            migration.executeUpdate();
        }

        if (trace) LOG.trace("[baseline] migration agg phase one - orderline_agg done!");

        conn.commit();
        return null;
	}
}
