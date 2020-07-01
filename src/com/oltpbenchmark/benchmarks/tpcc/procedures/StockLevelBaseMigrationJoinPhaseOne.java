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

public class StockLevelBaseMigrationJoinPhaseOne extends TPCCProcedure {

    private static final Logger LOG = Logger.getLogger(StockLevelBaseMigrationJoinPhaseOne.class);
    private static AtomicLong numRun = new AtomicLong(0);

    public SQLStmt migrationSQL = new SQLStmt(
            " insert into orderline_stock(" +
            " ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, " +
            " ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info, s_w_id, " +
            " s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, " +
            " s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, " +
            " s_dist_07, s_dist_08, s_dist_09, s_dist_10) " +
            " (select " +
            "  ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, " +
            "  ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info, s_w_id, " +
            "  s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, " +
            "  s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, " +
            "  s_dist_07, s_dist_08, s_dist_09, s_dist_10 " +
            "  from order_line, stock " +
            "  where ol_i_id = s_i_id);");

        // insert into orderline_stock(
        // ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d,
        // ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info, s_w_id,
        // s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data,
        // s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06,
        // s_dist_07, s_dist_08, s_dist_09, s_dist_10)
        // (select
        // ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d,
        // ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info, s_w_id,
        // s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data,
        // s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06,
        // s_dist_07, s_dist_08, s_dist_09, s_dist_10
        // from order_line, stock
        // where ol_i_id = s_i_id) on conflict (ol_w_id,ol_d_id,ol_o_id,ol_number,s_w_id,s_i_id) do nothing;

    private PreparedStatement migration = null;

    public ResultSet run(Connection conn, Random gen,
            int w_id, int numWarehouses,
            int terminalDistrictLowerID, int terminalDistrictUpperID,
            TPCCWorker w) throws SQLException {


        migration = this.getPreparedStatement(conn, migrationSQL);

        boolean trace = LOG.isTraceEnabled();

        if (numRun.getAndIncrement() == 0) {
            migration.executeUpdate();
        }

        if (trace) LOG.trace("[baseline] migration join phase one - orderline_stock done!");

        conn.commit();
        return null;
	 }
}
