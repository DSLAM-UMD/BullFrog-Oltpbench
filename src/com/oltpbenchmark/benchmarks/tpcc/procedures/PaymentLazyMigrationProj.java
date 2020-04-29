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
import java.util.ArrayList;
import java.util.Random;
import java.text.MessageFormat;

import org.apache.log4j.Logger;

import com.oltpbenchmark.DBWorkload;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.linkbench.LinkBenchConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConfig;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;

public class PaymentLazyMigrationProj extends TPCCProcedure {

    private static final Logger LOG = Logger.getLogger(PaymentLazyMigrationProj.class);

    public SQLStmt payUpdateWhseSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_WAREHOUSE + 
            "   SET W_YTD = W_YTD + ? " +
            " WHERE W_ID = ? ");
    
    public SQLStmt payGetWhseSQL = new SQLStmt(
            "SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME" + 
            "  FROM " + TPCCConstants.TABLENAME_WAREHOUSE + 
            " WHERE W_ID = ?");
    
    public SQLStmt payUpdateDistSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_DISTRICT + 
            "   SET D_YTD = D_YTD + ? " +
            " WHERE D_W_ID = ? " +
            "   AND D_ID = ?");
    
    public SQLStmt payGetDistSQL = new SQLStmt(
            "SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_NAME" + 
            "  FROM " + TPCCConstants.TABLENAME_DISTRICT + 
            " WHERE D_W_ID = ? " +
            "   AND D_ID = ?");

    public SQLStmt payGetCustSQL1 = new SQLStmt(
            "migrate 1 customer " +
            "explain select count(*) from customer_proj_v " +
            "where c_w_id = ?" +
            "  and c_d_id = ?" +
            "  and c_id = ?;");

    public String payGetCustSQL2 =
            "migrate insert into customer_proj(" +
            "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
            "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_street_1, " +
            "  c_city, c_state, c_zip, c_data) " +
            "(select " +
            "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
            "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_street_1, " +
            "  c_city, c_state, c_zip, c_data " +
            " from customer) ";

    public String payGetCustSQL = 
            " insert into customer_proj(" +
            "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
            "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_street_1, " +
            "  c_city, c_state, c_zip, c_data) " +
            "(select " +
            "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
            "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_street_1, " +
            "  c_city, c_state, c_zip, c_data " +
            "from customer " +
            "where c_w_id = {0,number,#} " +
            "  and c_d_id = {1,number,#} " +
            "  and c_id = {2,number,#});";

    public SQLStmt payGetCustSQL3 = new SQLStmt( 
            "SELECT C_FIRST, C_LAST, C_STREET_1, " + 
            "       C_CITY, C_STATE, C_ZIP, C_CREDIT, " + 
            "       C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA, C_ID " +
            "  FROM " + TPCCConstants.TABLENAME_CUSTOMER_PROJ + 
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_ID = ? FOR UPDATE");

    public SQLStmt payUpdateCustBalCdataSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER_PROJ + 
            "   SET C_BALANCE = ?, " +
            "       C_YTD_PAYMENT = ?, " + 
            "       C_PAYMENT_CNT = ?, " +
            "       C_DATA = ? " +
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " + 
            "   AND C_ID = ?");
        
    public SQLStmt payUpdateCustBalSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER_PROJ + 
            "   SET C_BALANCE = ?, " +
            "       C_YTD_PAYMENT = ?, " +
            "       C_PAYMENT_CNT = ? " +
            " WHERE C_W_ID = ? " + 
            "   AND C_D_ID = ? " + 
            "   AND C_ID = ?");
    
    public SQLStmt payInsertHistSQL = new SQLStmt(
            "INSERT INTO " + TPCCConstants.TABLENAME_HISTORY + 
            " (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) " +
            " VALUES (?,?,?,?,?,?,?,?)");

    public SQLStmt customerByNameSQL1 = new SQLStmt(
            "migrate 1 customer " +
            "explain select count(*) from customer_proj_v " +
            "where c_w_id = ?" +
            "  and c_d_id = ?" +
            "  and c_last = ?;");
        
    public String customerByNameSQL2 =
            "migrate insert into customer_proj(" +
            "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
            "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_street_1, " +
            "  c_city, c_state, c_zip, c_data) " +
            "(select " +
            "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
            "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_street_1, " +
            "  c_city, c_state, c_zip, c_data " +
            " from customer) ";
    
    public String customerByNameSQL = 
            " insert into customer_proj(" +
            "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
            "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_street_1, " +
            "  c_city, c_state, c_zip, c_data) " +
            "(select " +
            "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
            "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_street_1, " +
            "  c_city, c_state, c_zip, c_data " +
            "from customer " +
            "where c_w_id = {0,number,#} " +
            "  and c_d_id = {1,number,#} " +
            "  and c_last = ''{2}'');";

    public SQLStmt customerByNameSQL3 = new SQLStmt( 
            "SELECT C_FIRST, C_LAST, C_STREET_1, " + 
            "       C_CITY, C_STATE, C_ZIP, C_CREDIT, " + 
            "       C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA, C_ID " +
            "  FROM " + TPCCConstants.TABLENAME_CUSTOMER_PROJ + 
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_LAST = ? " +
            " ORDER BY C_FIRST FOR UPDATE;");

    // Payment Txn
    private PreparedStatement payUpdateWhse = null;
    private PreparedStatement payGetWhse = null;
    private PreparedStatement payUpdateDist = null;
    private PreparedStatement payInsertHist = null;
    private PreparedStatement payGetDist = null;
    private PreparedStatement payGetCust1 = null;
    // private PreparedStatement payGetCust2 = null;
    private PreparedStatement payGetCust3 = null;
    private PreparedStatement customerByName1 = null;
    // private PreparedStatement customerByName2 = null;
    private PreparedStatement customerByName3 = null;
    private PreparedStatement payUpdateCustBalCdata = null;
    private PreparedStatement payUpdateCustBal = null;
    private Statement stmt = null;

    public ResultSet run(Connection conn, Random gen,
                         int w_id, int numWarehouses,
                         int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) throws SQLException {

        if (DBWorkload.IS_CONFLICT) {
            payGetCustSQL2 =
                "migrate insert into customer_proj(" +
                "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
                "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_street_1, " +
                "  c_city, c_state, c_zip, c_data) " +
                "(select " +
                "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
                "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_street_1, " +
                "  c_city, c_state, c_zip, c_data " +
                " from customer) " +
                "on conflict (c_w_id,c_d_id,c_id) do nothing;";

            customerByNameSQL2 =
                "migrate insert into customer_proj(" +
                "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
                "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_street_1, " +
                "  c_city, c_state, c_zip, c_data) " +
                "(select " +
                "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
                "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_street_1, " +
                "  c_city, c_state, c_zip, c_data " +
                " from customer) " +
                "on conflict (c_w_id,c_d_id,c_id) do nothing;";            

            customerByNameSQL = 
                " insert into customer_proj(" +
                "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
                "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_street_1, " +
                "  c_city, c_state, c_zip, c_data) " +
                "(select " +
                "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
                "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_street_1, " +
                "  c_city, c_state, c_zip, c_data " +
                "from customer " +
                "where c_w_id = {0,number,#} " +
                "  and c_d_id = {1,number,#} " +
                "  and c_last = ''{2}'') " +
                "on conflict (c_w_id,c_d_id,c_id) do nothing;";

            payGetCustSQL = 
                " insert into customer_proj(" +
                "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
                "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_street_1, " +
                "  c_city, c_state, c_zip, c_data) " +
                "(select " +
                "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
                "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_street_1, " +
                "  c_city, c_state, c_zip, c_data " +
                "from customer " +
                "where c_w_id = {0,number,#} " +
                "  and c_d_id = {1,number,#} " +
                "  and c_id = {2,number,#}) " +
                "on conflict (c_w_id,c_d_id,c_id) do nothing;";
        }

        // initializing all prepared statements
        payUpdateWhse = this.getPreparedStatement(conn, payUpdateWhseSQL);
        payGetWhse = this.getPreparedStatement(conn, payGetWhseSQL);
        payUpdateDist = this.getPreparedStatement(conn, payUpdateDistSQL);
        payGetDist = this.getPreparedStatement(conn, payGetDistSQL);
        payInsertHist = this.getPreparedStatement(conn, payInsertHistSQL);
        payGetCust1 = this.getPreparedStatement(conn, payGetCustSQL1);
        // payGetCust2 = this.getPreparedStatement(conn, payGetCustSQL2);
        payGetCust3 = this.getPreparedStatement(conn, payGetCustSQL3);
        customerByName1 = this.getPreparedStatement(conn, customerByNameSQL1);
        // customerByName2 = this.getPreparedStatement(conn, customerByNameSQL2);
        customerByName3 = this.getPreparedStatement(conn, customerByNameSQL3);
        payUpdateCustBalCdata = this.getPreparedStatement(conn, payUpdateCustBalCdataSQL);
        payUpdateCustBal = this.getPreparedStatement(conn, payUpdateCustBalSQL);
        stmt = conn.createStatement();

        // payUpdateWhse =this.getPreparedStatement(conn, payUpdateWhseSQL);

        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
        int customerID = TPCCUtil.getCustomerID(gen);

        int x = TPCCUtil.randomNumber(1, 100, gen);
        int customerDistrictID;
        int customerWarehouseID;
        if (x <= 85) {
            customerDistrictID = districtID;
            customerWarehouseID = w_id;
        } else {
            customerDistrictID = TPCCUtil.randomNumber(1, TPCCConfig.configDistPerWhse, gen);
            do {
                customerWarehouseID = TPCCUtil.randomNumber(1, numWarehouses, gen);
            } while (customerWarehouseID == w_id && numWarehouses > 1);
        }

        long y = TPCCUtil.randomNumber(1, 100, gen);
        boolean customerByName;
        String customerLastName = null;
        customerID = -1;
        if (y <= 60) {
            // 60% lookups by last name
            customerByName = true;
            customerLastName = TPCCUtil.getNonUniformRandomLastNameForRun(gen);
        } else {
            // 40% lookups by customer ID
            customerByName = false;
            customerID = TPCCUtil.getCustomerID(gen);
        }

        // if (!customerByName) {
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
        //         customerWarehouseID = w.t_c_w_id.get().intValue(); 
        //         customerDistrictID = w.t_c_d_id.get().intValue(); 
        //         customerID = w.t_c_id.get().intValue(); 
        //         // LOG.info("worker_" + w.getId() + " (" + customerWarehouseID + "," + customerDistrictID + "," + customerID + ")");
        //     }
        // }

        float paymentAmount = (float) (TPCCUtil.randomNumber(100, 500000, gen) / 100.0);

        String w_street_1, w_street_2, w_city, w_state, w_zip, w_name;
        String d_street_1, d_street_2, d_city, d_state, d_zip, d_name;

        payUpdateWhse.setDouble(1, paymentAmount);
        payUpdateWhse.setInt(2, w_id);
        // MySQL reports deadlocks due to lock upgrades:
        // t1: read w_id = x; t2: update w_id = x; t1 update w_id = x
        int result = payUpdateWhse.executeUpdate();
        if (result == 0)
            throw new RuntimeException("W_ID=" + w_id + " not found!");

        payGetWhse.setInt(1, w_id);
        ResultSet rs = payGetWhse.executeQuery();
        if (!rs.next())
            throw new RuntimeException("W_ID=" + w_id + " not found!");
        w_street_1 = rs.getString("W_STREET_1");
        w_street_2 = rs.getString("W_STREET_2");
        w_city = rs.getString("W_CITY");
        w_state = rs.getString("W_STATE");
        w_zip = rs.getString("W_ZIP");
        w_name = rs.getString("W_NAME");
        rs.close();
        rs = null;

        payUpdateDist.setDouble(1, paymentAmount);
        payUpdateDist.setInt(2, w_id);
        payUpdateDist.setInt(3, districtID);
        result = payUpdateDist.executeUpdate();
        if (result == 0)
            throw new RuntimeException("D_ID=" + districtID + " D_W_ID=" + w_id + " not found!");

        payGetDist.setInt(1, w_id);
        payGetDist.setInt(2, districtID);
        rs = payGetDist.executeQuery();
        if (!rs.next())
            throw new RuntimeException("D_ID=" + districtID + " D_W_ID=" + w_id + " not found!");
        d_street_1 = rs.getString("D_STREET_1");
        d_street_2 = rs.getString("D_STREET_2");
        d_city = rs.getString("D_CITY");
        d_state = rs.getString("D_STATE");
        d_zip = rs.getString("D_ZIP");
        d_name = rs.getString("D_NAME");
        rs.close();
        rs = null;

        Customer c;
        if (customerByName) {
            assert customerID <= 0;
            ArrayList<Customer> customers = new ArrayList<Customer>();

            // customerByName1.setInt(1, customerWarehouseID);
            // customerByName1.setInt(2, customerDistrictID);
            // customerByName1.setString(3, customerLastName);
            customerByName3.setInt(1, customerWarehouseID);
            customerByName3.setInt(2, customerDistrictID);
            customerByName3.setString(3, customerLastName);
    
            // conn.setAutoCommit(false);
            // customerByName1.executeQuery();
            // stmt.executeUpdate(customerByNameSQL2);
            // conn.commit();

            conn.setAutoCommit(false);
            String migration = MessageFormat.format(customerByNameSQL,
                customerWarehouseID, customerDistrictID, customerLastName);
            stmt.executeUpdate(migration);
            conn.commit();
            
            rs = customerByName3.executeQuery();

            if (LOG.isTraceEnabled()) LOG.trace("C_LAST=" + customerLastName + " C_D_ID=" + customerDistrictID + " C_W_ID=" + customerWarehouseID);
    
            while (rs.next()) {
                customers.add(TPCCUtil.newCustomerFromResults2(rs));
            }
            rs.close();

            if (customers.size() == 0) {
                throw new RuntimeException("C_LAST=" + customerLastName + " C_D_ID=" + customerDistrictID + " C_W_ID=" + customerWarehouseID + " not found!");
            }
    
            // TPC-C 2.5.2.2: Position n / 2 rounded up to the next integer, but
            // that counts starting from 1.
            int index = customers.size() / 2;
            if (customers.size() % 2 == 0) {
                index -= 1;
            }
            c = customers.get(index);
        } else {
            assert customerLastName == null;
            // payGetCust1.setInt(1, customerWarehouseID);
            // payGetCust1.setInt(2, customerDistrictID);
            // payGetCust1.setInt(3, customerID);
            payGetCust3.setInt(1, customerWarehouseID);
            payGetCust3.setInt(2, customerDistrictID);
            payGetCust3.setInt(3, customerID);
            // conn.setAutoCommit(false);
            // payGetCust1.executeQuery();
            // stmt.executeUpdate(payGetCustSQL2);
            // conn.commit();

            conn.setAutoCommit(false);
            String migration = MessageFormat.format(payGetCustSQL,
                customerWarehouseID, customerDistrictID, customerID);
            stmt.executeUpdate(migration);
            conn.commit();            

            rs = payGetCust3.executeQuery();

            if (!rs.next()) {
                throw new RuntimeException("C_ID=" + customerID + " C_D_ID=" + customerDistrictID + " C_W_ID=" + customerWarehouseID + " not found!");
            }
    
            c = TPCCUtil.newCustomerFromResults2(rs);
            if (stmt != null) { stmt.close(); }
            rs.close();
        }

        c.c_balance -= paymentAmount;
        c.c_ytd_payment += paymentAmount;
        c.c_payment_cnt += 1;
        String c_data = null;
        if (c.c_credit.equals("BC")) { // bad credit
            rs = null;
            c_data = c.c_id + " " + customerDistrictID + " " + customerWarehouseID + " " + districtID + " " + w_id + " " + paymentAmount + " | " + c.c_data;
            if (c_data.length() > 500)
                c_data = c_data.substring(0, 500);

            payUpdateCustBalCdata.setDouble(1, c.c_balance);
            payUpdateCustBalCdata.setDouble(2, c.c_ytd_payment);
            payUpdateCustBalCdata.setInt(3, c.c_payment_cnt);
            payUpdateCustBalCdata.setString(4, c_data);
            payUpdateCustBalCdata.setInt(5, customerWarehouseID);
            payUpdateCustBalCdata.setInt(6, customerDistrictID);
            payUpdateCustBalCdata.setInt(7, c.c_id);
            result = payUpdateCustBalCdata.executeUpdate();

            if (result == 0)
                throw new RuntimeException("Error in PYMNT Txn updating Customer C_ID=" + c.c_id + " C_W_ID=" + customerWarehouseID + " C_D_ID=" + customerDistrictID);
        } else { // GoodCredit
            payUpdateCustBal.setDouble(1, c.c_balance);
            payUpdateCustBal.setDouble(2, c.c_ytd_payment);
            payUpdateCustBal.setInt(3, c.c_payment_cnt);
            payUpdateCustBal.setInt(4, customerWarehouseID);
            payUpdateCustBal.setInt(5, customerDistrictID);
            payUpdateCustBal.setInt(6, c.c_id);
            result = payUpdateCustBal.executeUpdate();

            if (result == 0)
                throw new RuntimeException("C_ID=" + c.c_id + " C_W_ID=" + customerWarehouseID + " C_D_ID=" + customerDistrictID + " not found!");
        }

        if (w_name.length() > 10)
            w_name = w_name.substring(0, 10);
        if (d_name.length() > 10)
            d_name = d_name.substring(0, 10);
        String h_data = w_name + "    " + d_name;

        payInsertHist.setInt(1, customerDistrictID);
        payInsertHist.setInt(2, customerWarehouseID);
        payInsertHist.setInt(3, c.c_id);
        payInsertHist.setInt(4, districtID);
        payInsertHist.setInt(5, w_id);
        payInsertHist.setTimestamp(6, w.getBenchmarkModule().getTimestamp(System.currentTimeMillis()));
        payInsertHist.setDouble(7, paymentAmount);
        payInsertHist.setString(8, h_data);
        payInsertHist.executeUpdate();

        conn.commit();

        if (LOG.isTraceEnabled()) {
            StringBuilder terminalMessage = new StringBuilder();
            terminalMessage.append("\n+---------------------------- PAYMENT ----------------------------+");
            terminalMessage.append("\n Date: " + TPCCUtil.getCurrentTime());
            terminalMessage.append("\n\n Warehouse: ");
            terminalMessage.append(w_id);
            terminalMessage.append("\n   Street:  ");
            terminalMessage.append(w_street_1);
            terminalMessage.append("\n   Street:  ");
            terminalMessage.append(w_street_2);
            terminalMessage.append("\n   City:    ");
            terminalMessage.append(w_city);
            terminalMessage.append("   State: ");
            terminalMessage.append(w_state);
            terminalMessage.append("  Zip: ");
            terminalMessage.append(w_zip);
            terminalMessage.append("\n\n District:  ");
            terminalMessage.append(districtID);
            terminalMessage.append("\n   Street:  ");
            terminalMessage.append(d_street_1);
            terminalMessage.append("\n   Street:  ");
            terminalMessage.append(d_street_2);
            terminalMessage.append("\n   City:    ");
            terminalMessage.append(d_city);
            terminalMessage.append("   State: ");
            terminalMessage.append(d_state);
            terminalMessage.append("  Zip: ");
            terminalMessage.append(d_zip);
            terminalMessage.append("\n\n Customer:  ");
            terminalMessage.append(c.c_id);
            terminalMessage.append("\n   Name:    ");
            terminalMessage.append(c.c_first);
            terminalMessage.append(" ");
            terminalMessage.append(c.c_last);
            terminalMessage.append("\n   Street:  ");
            terminalMessage.append(c.c_street_1);
            terminalMessage.append("\n   City:    ");
            terminalMessage.append(c.c_city);
            terminalMessage.append("   State: ");
            terminalMessage.append(c.c_state);
            terminalMessage.append("  Zip: ");
            terminalMessage.append(c.c_zip);
            terminalMessage.append("\n   Since:   ");
            if (c.c_since != null) {
                terminalMessage.append(c.c_since.toString());
            } else {
                terminalMessage.append("");
            }
            terminalMessage.append("\n   Credit:  ");
            terminalMessage.append(c.c_credit);
            terminalMessage.append("\n   %Disc:   ");
            terminalMessage.append(c.c_discount);
            terminalMessage.append("\n\n Amount Paid:      ");
            terminalMessage.append(paymentAmount);
            terminalMessage.append("\n New Cust-Balance: ");
            terminalMessage.append(c.c_balance);
            if (c.c_credit.equals("BC")) {
                if (c_data.length() > 50) {
                    terminalMessage.append("\n\n Cust-Data: " + c_data.substring(0, 50));
                    int data_chunks = c_data.length() > 200 ? 4 : c_data.length() / 50;
                    for (int n = 1; n < data_chunks; n++)
                        terminalMessage.append("\n            " + c_data.substring(n * 50, (n + 1) * 50));
                } else {
                    terminalMessage.append("\n\n Cust-Data: " + c_data);
                }
            }
            terminalMessage.append("\n+-----------------------------------------------------------------+\n\n");

            LOG.trace(terminalMessage.toString());
        }

        if (stmt != null) { stmt.close(); }
        return null;
    }
}
