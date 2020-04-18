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

public class StockLevelBaseMigrationSplit extends TPCCProcedure {

    private static final Logger LOG = Logger.getLogger(StockLevelBaseMigrationSplit.class);
    private static AtomicLong numRun = new AtomicLong(0);

    // DROP TABLE IF EXISTS order_line CASCADE;
    // CREATE TABLE order_line (
    //   ol_w_id int NOT NULL,
    //   ol_d_id int NOT NULL,
    //   ol_o_id int NOT NULL,
    //   ol_number int NOT NULL,
    //   ol_i_id int NOT NULL,
    //   ol_delivery_d timestamp NULL DEFAULT NULL,
    //   ol_amount decimal(6,2) NOT NULL,
    //   ol_supply_w_id int NOT NULL,
    //   ol_quantity decimal(2,0) NOT NULL,
    //   ol_dist_info char(24) NOT NULL,
    //   PRIMARY KEY (ol_w_id,ol_d_id,ol_o_id,ol_number)
    // );
    
    // DROP TABLE IF EXISTS stock CASCADE;
    // CREATE TABLE stock (
    //   s_w_id int NOT NULL,
    //   s_i_id int NOT NULL,
    //   s_quantity decimal(4,0) NOT NULL,
    //   s_ytd decimal(8,2) NOT NULL,
    //   s_order_cnt int NOT NULL,
    //   s_remote_cnt int NOT NULL,
    //   s_data varchar(50) NOT NULL,
    //   s_dist_01 char(24) NOT NULL,
    //   s_dist_02 char(24) NOT NULL,
    //   s_dist_03 char(24) NOT NULL,
    //   s_dist_04 char(24) NOT NULL,
    //   s_dist_05 char(24) NOT NULL,
    //   s_dist_06 char(24) NOT NULL,
    //   s_dist_07 char(24) NOT NULL,
    //   s_dist_08 char(24) NOT NULL,
    //   s_dist_09 char(24) NOT NULL,
    //   s_dist_10 char(24) NOT NULL,
    //   PRIMARY KEY (s_w_id,s_i_id)
    // );
    
    // CREATE INDEX FKEY_STOCK_2 ON STOCK (S_I_ID);
    // CREATE INDEX s_order_1 ON stock (s_w_id, s_quantity);
    // CREATE INDEX s_order_2 ON stock (s_w_id, s_i_id);
    
    // CREATE INDEX FKEY_ORDER_LINE_2 ON ORDER_LINE (OL_I_ID);
    // CREATE INDEX ol_order_1 ON order_line (ol_o_id, ol_d_id, ol_w_id);

    public SQLStmt deleteOrderlineSQL = new SQLStmt(
            "delete from order_line");

    public SQLStmt deleteStockSQL = new SQLStmt(
            "delete from stock");

    public SQLStmt migrateOrderlineSQL = new SQLStmt(
            "insert into order_line(" +
            "  ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, " +
            "  ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info)" +
            " (select " +
            "  ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, " +
            "  ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info " +
            "  from orderline_stock) on conflict (ol_w_id,ol_d_id,ol_o_id,ol_number) do nothing;");

        //     insert into order_line(
        //         ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d,
        //         ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info)
        //         (select
        //         ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d,
        //         ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info
        //         from orderline_stock) on conflict (ol_w_id,ol_d_id,ol_o_id,ol_number) do nothing;

    public SQLStmt migrateStockSQL = new SQLStmt(
            "insert into stock(" +
            " s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, " +
            "  s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, " +
            "  s_dist_07, s_dist_08, s_dist_09, s_dist_10) " +
            " (select " +
            "  s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, " +
            "  s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, " +
            "  s_dist_07, s_dist_08, s_dist_09, s_dist_10 " +
            "  from orderline_stock) on conflict (s_w_id,s_i_id) do nothing;");

        //     insert into stock(
        //         s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data,
        //         s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06,
        //         s_dist_07, s_dist_08, s_dist_09, s_dist_10)
        //         (select
        //         s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data,
        //         s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06,
        //         s_dist_07, s_dist_08, s_dist_09, s_dist_10
        //         from orderline_stock) on conflict (s_w_id,s_i_id) do nothing;

    private PreparedStatement deleteOrderline = null;
    private PreparedStatement deleteStock = null;
    private PreparedStatement migrateOrderline = null;
    private PreparedStatement migrateStock = null;

    public ResultSet run(Connection conn, Random gen,
            int w_id, int numWarehouses,
            int terminalDistrictLowerID, int terminalDistrictUpperID,
            TPCCWorker w) throws SQLException {

        boolean trace = LOG.isTraceEnabled();

        deleteOrderline = this.getPreparedStatement(conn, deleteOrderlineSQL);
        deleteStock = this.getPreparedStatement(conn, deleteStockSQL);
        migrateOrderline = this.getPreparedStatement(conn, migrateOrderlineSQL);
        migrateStock = this.getPreparedStatement(conn, migrateStockSQL);

        if (numRun.getAndIncrement() == 0) {
            deleteOrderline.executeUpdate();
            deleteStock.executeUpdate();
            migrateOrderline.executeUpdate();
            migrateStock.executeUpdate();
        }

        if (trace) LOG.trace("[baseline] migration proj phase one - customer_proj done!");

        conn.commit();
        return null;
	}
}
