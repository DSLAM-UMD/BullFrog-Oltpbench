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
import java.sql.Statement;
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

public class StockLevelBaseMigrationProjPhaseOne extends TPCCProcedure {

    private static final Logger LOG = Logger.getLogger(StockLevelBaseMigrationProjPhaseOne.class);
    private static AtomicLong numRun = new AtomicLong(0);

    public String migrationSQL1 = 
            " insert into customer_proj1(" +
            "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
            "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) " +
            "(select " +
            "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
            "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data " +
            "from customer) " +
            "on conflict (c_w_id,c_d_id,c_id) do nothing;";

    public String migrationSQL2 = 
            " insert into customer_proj2(" +
            "  c_w_id, c_d_id, c_id, c_last, c_first, " +
            "  c_street_1, c_city, c_state, c_zip) " +
            "(select " +
            "  c_w_id, c_d_id, c_id, c_last, c_first, " +
            "  c_street_1, c_city, c_state, c_zip " +
            "from customer) " +
            "on conflict (c_w_id,c_d_id,c_id) do nothing;";

            // insert into customer_proj1(
            //     c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first,
            //     c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data)
            //     (select
            //     c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first,
            //     c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data
            //     from customer); 

            // insert into customer_proj2(
            //     c_w_id, c_d_id, c_id, c_last, c_first,
            //     c_street_1, c_city, c_state, c_zip)
            //     (select
            //     c_w_id, c_d_id, c_id, c_last, c_first,
            //     c_street_1, c_city, c_state, c_zip
            //     from customer); 

    private Statement stmt = null;

    public ResultSet run(Connection conn, Random gen,
            int w_id, int numWarehouses,
            int terminalDistrictLowerID, int terminalDistrictUpperID,
            TPCCWorker w) throws SQLException {

        boolean trace = LOG.isTraceEnabled();

        stmt = conn.createStatement();

        if (numRun.getAndIncrement() == 0) {
            long startTime = System.currentTimeMillis();
            stmt.addBatch(migrationSQL1);
            stmt.addBatch(migrationSQL2);
            stmt.executeBatch();
            long endTime = System.currentTimeMillis();
            LOG.info("[baseline] eager migration time: " + (endTime - startTime) + "ms");
        }

        if (trace) LOG.trace("[baseline] migration proj phase one - customer_proj done!");

        conn.commit();
        return null;
	}
}
