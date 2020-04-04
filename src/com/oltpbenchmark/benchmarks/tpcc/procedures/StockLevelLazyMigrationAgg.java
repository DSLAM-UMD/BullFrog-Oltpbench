/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
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

import org.apache.log4j.Logger;

import com.oltpbenchmark.DBWorkload;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;

public class StockLevelLazyMigrationAgg extends TPCCProcedure {

    private static final Logger LOG = Logger.getLogger(StockLevelLazyMigrationAgg.class);

	public SQLStmt stockGetDistOrderIdSQL = new SQLStmt(
	        "SELECT D_NEXT_O_ID " + 
            "  FROM " + TPCCConstants.TABLENAME_DISTRICT +
	        " WHERE D_W_ID = ? " +
            "   AND D_ID = ?");

	public SQLStmt stockGetCountStockSQL = new SQLStmt(
	        "SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT " +
			" FROM " + TPCCConstants.TABLENAME_ORDERLINE + ", " + TPCCConstants.TABLENAME_STOCK +
			" WHERE OL_W_ID = ?" +
			" AND OL_D_ID = ?" +
			" AND OL_O_ID < ?" +
			" AND OL_O_ID >= ?" +
			" AND S_W_ID = ?" +
			" AND S_I_ID = OL_I_ID" + 
			" AND S_QUANTITY < ?");

    String txnFormat = 
            "migrate 1 order_line " +
            " explain select count(*) from orderline_agg_v " + 
            " where ol_w_id = {0,number,#} " +
            " and ol_d_id = {1,number,#} " +
            " and ol_o_id < {2,number,#} " +
            " and ol_o_id >= {3,number,#}; "
            +
            "migrate insert into orderline_agg(" +
            " ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, " +
            " ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info) " +
            " (select " +
            "  sum(ol_w_id), ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, " +
            "  ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info " +
            "  from order_line " +
            "  group by ol_w_id, ol_d_id, ol_o_id, ol_number) " + 
            "  ON CONFLICT (ol_w_id,ol_d_id,ol_o_id,ol_number) DO NOTHING; "
            +
            "select count(distinct (s_i_id)) as stock_count " +
            " FROM " + TPCCConstants.TABLENAME_ORDERLINE_AGG + ", " + TPCCConstants.TABLENAME_STOCK +
            " where ol_w_id = {4,number,#} " +
            " and ol_d_id = {5,number,#} " +
            " and ol_o_id < {6,number,#} " +
            " and ol_o_id >= {7,number,#} " +
            " and s_w_id = {8,number,#} " +
            " and s_i_id = ol_i_id " + 
            " and s_quantity < {9,number,#};";

	// Stock Level Txn
	private PreparedStatement stockGetDistOrderId = null;
	private PreparedStatement stockGetCountStock = null;

	 public ResultSet run(Connection conn, Random gen,
				int w_id, int numWarehouses,
				int terminalDistrictLowerID, int terminalDistrictUpperID,
				TPCCWorker w) throws SQLException {

	     boolean trace = LOG.isTraceEnabled(); 
	     
	     stockGetDistOrderId = this.getPreparedStatement(conn, stockGetDistOrderIdSQL);
	     stockGetCountStock= this.getPreparedStatement(conn, stockGetCountStockSQL);

	     int threshold = TPCCUtil.randomNumber(10, 20, gen);
	     int d_id = TPCCUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);

	     int o_id = 0;
	     // XXX int i_id = 0;
	     int stock_count = 0;

	     stockGetDistOrderId.setInt(1, w_id);
         stockGetDistOrderId.setInt(2, d_id);
         if (trace) LOG.trace(String.format("stockGetDistOrderId BEGIN [W_ID=%d, D_ID=%d]", w_id, d_id));
         ResultSet rs = stockGetDistOrderId.executeQuery();
         if (trace) LOG.trace("stockGetDistOrderId END");

         if (!rs.next()) {
             throw new RuntimeException("D_W_ID="+ w_id +" D_ID="+ d_id+" not found!");
         }
         o_id = rs.getInt("D_NEXT_O_ID");
         rs.close();

        //  stockGetCountStock.setInt(1, w_id);
        //  stockGetCountStock.setInt(2, d_id);
        //  stockGetCountStock.setInt(3, o_id);
        //  stockGetCountStock.setInt(4, o_id - 20);
        //  stockGetCountStock.setInt(5, w_id);
        //  stockGetCountStock.setInt(6, threshold);
        //  if (trace) LOG.trace(String.format("stockGetCountStock BEGIN [W_ID=%d, D_ID=%d, O_ID=%d]", w_id, d_id, o_id));
        //  rs = stockGetCountStock.executeQuery();
        //  if (trace) LOG.trace("stockGetCountStock END");

        //  if (!rs.next()) {
        //      String msg = String.format("Failed to get StockLevel result for COUNT query " +
        //                                 "[W_ID=%d, D_ID=%d, O_ID=%d]", w_id, d_id, o_id);
        //      if (trace) LOG.warn(msg);
        //      throw new RuntimeException(msg);
        //  }
        //  stock_count = rs.getInt("STOCK_COUNT");
        //  if (trace) LOG.trace("stockGetCountStock RESULT=" + stock_count);

        String migration = MessageFormat.format(txnFormat,
            w_id, d_id, o_id, o_id - 20, 
            w_id, d_id, o_id, o_id - 20, w_id, threshold);
        
        String[] command = {"/bin/sh", "-c",
            "echo \"" + migration + "\" | " +
            DBWorkload.DB_BINARY_PATH + "/psql -qS -1 -p " +
            DBWorkload.DB_PORT_NUMBER + " tpcc"};
        execCommands(command);

         conn.commit();
        //  rs.close();

         if (trace) {
             StringBuilder terminalMessage = new StringBuilder();
             terminalMessage.append("\n+-------------------------- STOCK-LEVEL --------------------------+");
             terminalMessage.append("\n Warehouse: ");
             terminalMessage.append(w_id);
             terminalMessage.append("\n District:  ");
             terminalMessage.append(d_id);
             terminalMessage.append("\n\n Stock Level Threshold: ");
             terminalMessage.append(threshold);
             terminalMessage.append("\n Low Stock Count:       ");
             terminalMessage.append(stock_count);
             terminalMessage.append("\n+-----------------------------------------------------------------+\n\n");
             LOG.trace(terminalMessage.toString());
         }
         return null;
	 }
}
