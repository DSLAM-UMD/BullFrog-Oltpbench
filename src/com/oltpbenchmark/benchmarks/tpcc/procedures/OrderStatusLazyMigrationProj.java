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
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Random;
import java.text.MessageFormat;

import org.apache.log4j.Logger;

import com.oltpbenchmark.DBWorkload;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;

public class OrderStatusLazyMigrationProj extends TPCCProcedure {

    private static final Logger LOG = Logger.getLogger(OrderStatusLazyMigrationProj.class);

	public SQLStmt ordStatGetNewestOrdSQL = new SQLStmt(
	        "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D " +
            "  FROM " + TPCCConstants.TABLENAME_OPENORDER + 
            " WHERE O_W_ID = ? " + 
            "   AND O_D_ID = ? " + 
            "   AND O_C_ID = ? " +
            " ORDER BY O_ID DESC LIMIT 1");

	public SQLStmt ordStatGetOrderLinesSQL = new SQLStmt(
	        "SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D " + 
            "  FROM " + TPCCConstants.TABLENAME_ORDERLINE + 
            " WHERE OL_O_ID = ?" + 
            "   AND OL_D_ID = ?" + 
            "   AND OL_W_ID = ?");

    public String payGetCustSQL1 = 
            " insert into customer_proj1(" +
            "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
            "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) " +
            "(select " +
            "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
            "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data " +
            "from customer " +
            "where c_w_id = {0,number,#} " +
            "  and c_d_id = {1,number,#} " +
            "  and c_id = {2,number,#});";

    public String payGetCustSQL2 = 
            " insert into customer_proj2(" +
            "  c_w_id, c_d_id, c_id, c_last, c_first, " +
            "  c_street_1, c_city, c_state, c_zip) " +
            "(select " +
            "  c_w_id, c_d_id, c_id, c_last, c_first, " +
            "  c_street_1, c_city, c_state, c_zip " +
            "from customer " +
            "where c_w_id = {0,number,#} " +
            "  and c_d_id = {1,number,#} " +
            "  and c_id = {2,number,#});";

    public String payGetCustSQL3 = 
	        "SELECT p1.C_FIRST, p1.C_LAST, C_STREET_1, " +
            "       C_CITY, C_STATE, C_ZIP, C_CREDIT, " +
            "       C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA, p1.C_ID " +
            "  FROM customer_proj1 p1, customer_proj2 p2 " +
            " WHERE p1.c_w_id = {0,number,#} AND p1.c_w_id = p2.c_w_id " +
            "   AND p1.c_d_id = {1,number,#} AND p1.c_d_id = p2.c_d_id " +
            "   AND p1.c_id = {2,number,#} AND p1.c_id = p2.c_id FOR UPDATE;";

    public String customerByNameSQL1 = 
            " insert into customer_proj1(" +
            "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
            "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) " +
            "(select " +
            "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
            "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data " +
            "from customer " +
            "where c_w_id = {0,number,#} " +
            "  and c_d_id = {1,number,#} " +
            "  and c_last = ''{2}'');";
        
    public String customerByNameSQL2 = 
            " insert into customer_proj2(" +
            "  c_w_id, c_d_id, c_id, c_last, c_first, " +
            "  c_street_1, c_city, c_state, c_zip) " +
            "(select " +
            "  c_w_id, c_d_id, c_id, c_last, c_first, " +
            "  c_street_1, c_city, c_state, c_zip " +
            "from customer " +
            "where c_w_id = {0,number,#} " +
            "  and c_d_id = {1,number,#} " +
            "  and c_last = ''{2}'');";

    public String customerByNameSQL3 =
	        "SELECT p1.C_FIRST, C_STREET_1, C_CITY, " +
            "       C_STATE, C_ZIP, C_CREDIT, C_DISCOUNT, " +
            "       C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA, p1.C_ID " +
            "  FROM customer_proj1 p1, customer_proj2 p2 " +
            " WHERE p1.C_W_ID = {0,number,#} AND p1.C_W_ID = p2.C_W_ID " +
            "   AND p1.C_D_ID = {1,number,#} AND p1.C_D_ID = p2.C_D_ID " +
            "   AND p1.C_LAST = ''{2}'' AND p1.C_LAST = p2.C_LAST " +
            " ORDER BY p1.C_FIRST FOR UPDATE";

	private PreparedStatement ordStatGetNewestOrd = null;
	private PreparedStatement ordStatGetOrderLines = null;
    private Statement stmt = null;


    public ResultSet run(Connection conn, Random gen, int w_id, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) throws SQLException {
        boolean trace = LOG.isTraceEnabled();
        
        if (DBWorkload.IS_CONFLICT) {
            payGetCustSQL1 = 
                " insert into customer_proj1(" +
                "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
                "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) " +
                "(select " +
                "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
                "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data " +
                "from customer " +
                "where c_w_id = {0,number,#} " +
                "  and c_d_id = {1,number,#} " +
                "  and c_id = {2,number,#}) " +
                "on conflict (c_w_id,c_d_id,c_id) do nothing;";;

            payGetCustSQL2 = 
                " insert into customer_proj2(" +
                "  c_w_id, c_d_id, c_id, c_last, c_first, " +
                "  c_street_1, c_city, c_state, c_zip) " +
                "(select " +
                "  c_w_id, c_d_id, c_id, c_last, c_first, " +
                "  c_street_1, c_city, c_state, c_zip " +
                "from customer " +
                "where c_w_id = {0,number,#} " +
                "  and c_d_id = {1,number,#} " +
                "  and c_id = {2,number,#}) " +
                "on conflict (c_w_id,c_d_id,c_id) do nothing;";

            customerByNameSQL1 = 
                " insert into customer_proj1(" +
                "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
                "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) " +
                "(select " +
                "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
                "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data " +
                "from customer " +
                "where c_w_id = {0,number,#} " +
                "  and c_d_id = {1,number,#} " +
                "  and c_last = ''{2}'') " +
                "on conflict (c_w_id,c_d_id,c_id) do nothing;";
            
            customerByNameSQL2 = 
                " insert into customer_proj2(" +
                "  c_w_id, c_d_id, c_id, c_last, c_first, " +
                "  c_street_1, c_city, c_state, c_zip) " +
                "(select " +
                "  c_w_id, c_d_id, c_id, c_last, c_first, " +
                "  c_street_1, c_city, c_state, c_zip " +
                "from customer " +
                "where c_w_id = {0,number,#} " +
                "  and c_d_id = {1,number,#} " +
                "  and c_last = ''{2}'') " +
                "on conflict (c_w_id,c_d_id,c_id) do nothing;";
        }

        // initializing all prepared statements
        ordStatGetNewestOrd = this.getPreparedStatement(conn, ordStatGetNewestOrdSQL);
        ordStatGetOrderLines = this.getPreparedStatement(conn, ordStatGetOrderLinesSQL);
        stmt = conn.createStatement();

        int d_id = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
        boolean c_by_name = false;
        int y = TPCCUtil.randomNumber(1, 100, gen);
        String c_last = null;
        int c_id = -1;
        if (y <= 60) {
            c_by_name = true;
            c_last = TPCCUtil.getNonUniformRandomLastNameForRun(gen);
        } else {
            c_by_name = false;
            c_id = TPCCUtil.getCustomerID(gen);
        }

        // if (!c_by_name) {
        //     int probability = TPCCUtil.randomNumber(1, 100, gen); 
        //     if (probability <= 80) {
        //         if(w.t_c_w_id.get() == null) {
        //             w.t_c_w_id.set(w.getId() * 6 + 1); // w.getId() returns worker id.
        //             w.t_c_d_id.set(1);
        //             w.t_c_id.set(1);
        //         } else {
        //             // I didn't use a Bloom Filter to answer the question of whether or not a given tuple is migrated.
        //             // It is not necessary to always choose nonmigrated tuples since the repeated txn is still a txn
        //             // which will be counted into the throughput. As long as the migration is performed in order, it
        //             // will be completed sooner or later.
        //             if (w.t_c_id.get().intValue() == 3000) {
        //                 if (w.t_c_d_id.get().intValue() == 10) {
        //                     w.t_c_w_id.set(w.t_c_w_id.get().intValue() + 1); // auto increment
        //                     w.t_c_d_id.set(1);
        //                     w.t_c_id.set(1);                    
        //                 } else {
        //                     w.t_c_d_id.set(w.t_c_d_id.get().intValue() + 1); // auto increment
        //                     w.t_c_id.set(1);
        //                 }
        //             } else {
        //                 w.t_c_id.set(w.t_c_id.get().intValue() + 1); // auto increment 
        //             }
        //         }
        //         // assign a determinstic predicate
        //         w_id = w.t_c_w_id.get().intValue(); 
        //         d_id = w.t_c_d_id.get().intValue(); 
        //         c_id = w.t_c_id.get().intValue(); 
        //         // LOG.info("worker_" + w.getId() + " (" + w_id + "," + d_id + "," + c_id + ")");
        //     }
        // }

        int o_id = -1, o_carrier_id = -1;
        Timestamp o_entry_d;
        ArrayList<String> orderLines = new ArrayList<String>();

        Customer c;
        if (c_by_name) {
            assert c_id <= 0;
            // TODO: This only needs c_balance, c_first, c_middle, c_id
            // only fetch those columns?
            ArrayList<Customer> customers = new ArrayList<Customer>();

            if (!DBWorkload.IS_CONFLICT)
                conn.setAutoCommit(false);
            stmt.addBatch(MessageFormat.format(customerByNameSQL1, w_id, d_id, c_last));
            stmt.addBatch(MessageFormat.format(customerByNameSQL2, w_id, d_id, c_last));
            stmt.executeBatch();
            if (!DBWorkload.IS_CONFLICT)
                conn.commit();

            // Extract condition expressions and add it to lazy query
            String mQuery = MessageFormat.format(customerByNameSQL3,
                w_id, d_id, c_last);
            ResultSet rs = stmt.executeQuery(mQuery);

            if (LOG.isTraceEnabled()) LOG.trace("C_LAST=" + c_last + " C_D_ID=" + d_id + " C_W_ID=" + w_id);
    
            while (rs.next()) {
                customers.add(TPCCUtil.newCustomerFromResults2(rs));
            }
            rs.close();

            if (customers.size() == 0) {
                // throw new RuntimeException("C_LAST=" + c_last + " C_D_ID=" + d_id + " C_W_ID=" + w_id + " not found!");
                return null;
            }
    
            // TPC-C 2.5.2.2: Position n / 2 rounded up to the next integer, but
            // that counts starting from 1.
            int index = customers.size() / 2;
            if (customers.size() % 2 == 0) {
                index -= 1;
            }
            c = customers.get(index);
        } else {
            assert c_last == null;
            if (!DBWorkload.IS_CONFLICT)
                conn.setAutoCommit(false);
            stmt.addBatch(MessageFormat.format(payGetCustSQL1, w_id, d_id, c_id));
            stmt.addBatch(MessageFormat.format(payGetCustSQL2, w_id, d_id, c_id));
            stmt.executeBatch();
            if (!DBWorkload.IS_CONFLICT)
                conn.commit();

            // Extract condition expressions and add it to lazy query
            String mQuery = MessageFormat.format(payGetCustSQL3,
                w_id, d_id, c_id);
            ResultSet rs = stmt.executeQuery(mQuery);

            if (!rs.next()) {
                // throw new RuntimeException("C_ID=" + c_id + " C_D_ID=" + d_id + " C_W_ID=" + w_id + " not found!");
                return null;
            }
    
            c = TPCCUtil.newCustomerFromResults2(rs);
            rs.close();
        }

        // find the newest order for the customer
        // retrieve the carrier & order date for the most recent order.

        ordStatGetNewestOrd.setInt(1, w_id);
        ordStatGetNewestOrd.setInt(2, d_id);
        ordStatGetNewestOrd.setInt(3, c.c_id);
        if (trace) LOG.trace("ordStatGetNewestOrd START");
        ResultSet rs = ordStatGetNewestOrd.executeQuery();
        if (trace) LOG.trace("ordStatGetNewestOrd END");

        if (!rs.next()) {
            String msg = String.format("No order records for CUSTOMER [C_W_ID=%d, C_D_ID=%d, C_ID=%d]",
                                       w_id, d_id, c.c_id);
            if (trace) LOG.warn(msg);
            throw new RuntimeException(msg);
        }

        o_id = rs.getInt("O_ID");
        o_carrier_id = rs.getInt("O_CARRIER_ID");
        o_entry_d = rs.getTimestamp("O_ENTRY_D");
        rs.close();

        // retrieve the order lines for the most recent order
        ordStatGetOrderLines.setInt(1, o_id);
        ordStatGetOrderLines.setInt(2, d_id);
        ordStatGetOrderLines.setInt(3, w_id);
        if (trace) LOG.trace("ordStatGetOrderLines START");
        rs = ordStatGetOrderLines.executeQuery();
        if (trace) LOG.trace("ordStatGetOrderLines END");

        while (rs.next()) {
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            sb.append(rs.getLong("OL_SUPPLY_W_ID"));
            sb.append(" - ");
            sb.append(rs.getLong("OL_I_ID"));
            sb.append(" - ");
            sb.append(rs.getLong("OL_QUANTITY"));
            sb.append(" - ");
            sb.append(TPCCUtil.formattedDouble(rs.getDouble("OL_AMOUNT")));
            sb.append(" - ");
            if (rs.getTimestamp("OL_DELIVERY_D") != null)
                sb.append(rs.getTimestamp("OL_DELIVERY_D"));
            else
                sb.append("99-99-9999");
            sb.append("]");
            orderLines.add(sb.toString());
        }
        rs.close();
        rs = null;

        // commit the transaction
        conn.commit();
        
        if (orderLines.isEmpty()) {
            String msg = String.format("Order record had no order line items [C_W_ID=%d, C_D_ID=%d, C_ID=%d, O_ID=%d]",
                                       w_id, d_id, c.c_id, o_id);
            if (trace) LOG.warn(msg);
        }

        if (trace) {
            StringBuilder sb = new StringBuilder();
            sb.append("\n");
            sb.append("+-------------------------- ORDER-STATUS -------------------------+\n");
            sb.append(" Date: ");
            sb.append(TPCCUtil.getCurrentTime());
            sb.append("\n\n Warehouse: ");
            sb.append(w_id);
            sb.append("\n District:  ");
            sb.append(d_id);
            sb.append("\n\n Customer:  ");
            sb.append(c.c_id);
            sb.append("\n   Name:    ");
            sb.append(c.c_first);
            sb.append(" ");
            sb.append(c.c_last);
            sb.append("\n   Balance: ");
            sb.append(c.c_balance);
            sb.append("\n\n");
            if (o_id == -1) {
                sb.append(" Customer has no orders placed.\n");
            } else {
                sb.append(" Order-Number: ");
                sb.append(o_id);
                sb.append("\n    Entry-Date: ");
                sb.append(o_entry_d);
                sb.append("\n    Carrier-Number: ");
                sb.append(o_carrier_id);
                sb.append("\n\n");
                if (orderLines.size() != 0) {
                    sb.append(" [Supply_W - Item_ID - Qty - Amount - Delivery-Date]\n");
                    for (String orderLine : orderLines) {
                        sb.append(" ");
                        sb.append(orderLine);
                        sb.append("\n");
                    }
                } else {
                    LOG.trace(" This Order has no Order-Lines.\n");
                }
            }
            sb.append("+-----------------------------------------------------------------+\n\n");
            LOG.trace(sb.toString());
        }

        if (stmt != null) { stmt.close(); }
        return null;
    }
}
