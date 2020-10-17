package com.oltpbenchmark;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.log4j.Logger;

import java.text.MessageFormat;

import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.SQLException;

import com.oltpbenchmark.DBWorkload;

public class BgThread2 extends Thread {

    private static final Logger LOG = Logger.getLogger(BgThread2.class);
    private volatile boolean flag = true;

    private Connection conn = null;
    private Statement stmt = null;
    private String migrationFmt = null;
    private String projection1 = null;
    private String projection2 = null;
    private String projection3 = null;
    private String projection4 = null;

    private ThreadBench threadBench = null;

    public BgThread2(String name, ThreadBench bench) {
        super(name);
        threadBench = bench;
    }

    public void stopRunning() {
        flag = false;
    }

    public synchronized void run() {
        if (DBWorkload.BACKGROUND_THREAD != null) {
            if (DBWorkload.BACKGROUND_THREAD.equals("projection") || DBWorkload.BACKGROUND_THREAD.equals("proj")) {
                projection1 = 
                    " insert into customer_proj1(" +
                    "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
                    "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) " +
                    "(select " +
                    "  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance, " +
                    "  c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data " +
                    "from customer " +
                    "where c_w_id = {0,number,#} " +
                    "  and c_d_id = {1,number,#}) ";
    
                projection2 = 
                    " insert into customer_proj2(" +
                    "  c_w_id, c_d_id, c_id, c_last, c_first, " +
                    "  c_street_1, c_city, c_state, c_zip) " +
                    "(select " +
                    "  c_w_id, c_d_id, c_id, c_last, c_first, " +
                    "  c_street_1, c_city, c_state, c_zip " +
                    "from customer " +
                    "where c_w_id = {0,number,#} " +
                    "  and c_d_id = {1,number,#}) ";

                // projection3 =
                //     " insert into district1(" +
                //     "  d_w_id, d_id, d_ytd, d_tax, d_next_o_id, " +
                //     "  d_name, d_street_1, d_street_2, d_city, d_state, d_zip) " +
                //     "(select " +
                //     "  d_w_id, d_id, d_ytd, d_tax, d_next_o_id, " +
                //     "  d_name, d_street_1, d_street_2, d_city, d_state, d_zip " +
                //     "from district " +
                //     "where d_w_id = {0,number,#} " +
                //     "  and d_id = {1,number,#}) " +
                //     "on conflict (d_w_id,d_id) do nothing;";

                // projection4 =
                //     " insert into oorder1(" +
                //     "  o_w_id, o_d_id, o_c_id, o_id, " +
                //     "  o_carrier_id, o_ol_cnt, o_all_local, o_entry_d) " +
                //     "(select " +
                //     "  o_w_id, o_d_id, o_c_id, o_id, " +
                //     "  o_carrier_id, o_ol_cnt, o_all_local, o_entry_d " +
                //     "from oorder " +
                //     "where o_w_id = {0,number,#} " +
                //     "  and o_d_id = {1,number,#}) " +
                //     "on conflict (o_w_id,o_d_id,o_id) do nothing;";

                if (DBWorkload.IS_CONFLICT) {
                    projection1 += " on conflict (c_w_id,c_d_id,c_id) do nothing;";
                    projection2 += " on conflict (c_w_id,c_d_id,c_id) do nothing;";
                }
            }
            if (DBWorkload.BACKGROUND_THREAD.equals("aggregation") || DBWorkload.BACKGROUND_THREAD.equals("agg")) {
                migrationFmt =                     
                    " insert into orderline_agg(" +
                    " ol_amount_sum, ol_quantity_avg, ol_o_id, ol_d_id, ol_w_id) " +    
                    " (select sum(ol_amount), avg(ol_quantity), ol_o_id, ol_d_id, ol_w_id " +
                    " from order_line where ol_w_id = {0,number,#} and ol_d_id = {1,number,#}" + 
                    " group by ol_o_id, ol_d_id, ol_w_id)";
                if (DBWorkload.IS_CONFLICT) {
                    migrationFmt += " on conflict (ol_o_id, ol_d_id, ol_w_id) do nothing;";
                }
            }
            if (DBWorkload.BACKGROUND_THREAD.equals("join")) {
                migrationFmt =                     
                    "insert into orderline_stock(" +
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
                    "  where ol_w_id = {0,number,#} " +
                    "  and ol_d_id = {1,number,#} " +
                    "  and ol_o_id > {2,number,#} " +
                    "  and ol_o_id <= {3,number,#} " +
                    "  and ol_i_id = s_i_id) ";
                if (DBWorkload.IS_CONFLICT) {
                    migrationFmt += " ON CONFLICT (ol_w_id,ol_d_id,ol_o_id,ol_number,s_w_id,s_i_id) "
                                 +  " DO NOTHING;";
                }
            }
        }

        Connection c = null;
        try {
           Class.forName("org.postgresql.Driver");
           c = DriverManager
              .getConnection("jdbc:postgresql://localhost:" +
              DBWorkload.DB_PORT_NUMBER + "/tpcc",
              "postgres", "postgres");
        } catch (Exception e) {
           e.printStackTrace();
           System.err.println(e.getClass().getName() + ": " + e.getMessage());
           System.exit(0);
        }

        try { 
            stmt = c.createStatement();
            if (DBWorkload.BACKGROUND_THREAD != null) {
                if (DBWorkload.BACKGROUND_THREAD.equals("projection")  || DBWorkload.BACKGROUND_THREAD.equals("proj")) {
                    for (int c_w_id = 50; c_w_id > 25; c_w_id--) {
                        for (int c_d_id = 1; c_d_id <= 10; c_d_id++) {
                            // <= 3000 tuples will be migrated each time
                            String migration = MessageFormat.format(projection1, c_w_id, c_d_id);
                            LOG.info(migration);
                            stmt.addBatch(migration);
                            migration = MessageFormat.format(projection2, c_w_id, c_d_id);
                            LOG.info(migration);
                            stmt.addBatch(migration);
                            stmt.executeBatch();
                            // migration = MessageFormat.format(projection3, c_w_id, c_d_id);
                            // LOG.info(migration);
                            // stmt.addBatch(migration);
                            // migration = MessageFormat.format(projection4, c_w_id, c_d_id);
                            // LOG.info(migration);
                            // stmt.addBatch(migration);
                            // threadBench.increment();
                            // threadBench.increment();
                            Thread.sleep(300);
                            if (!flag) break;
                        }
                        if (!flag) break;
                    }
                }
                 if (DBWorkload.BACKGROUND_THREAD.equals("aggregation") || DBWorkload.BACKGROUND_THREAD.equals("agg")) {
                    for (int c_w_id = 50; c_w_id > 25; c_w_id--) {
                        for (int c_d_id = 1; c_d_id <= 10; c_d_id++) {
                            // <= 3000 tuples will be migrated each time
                            String migration = MessageFormat.format(migrationFmt, c_w_id, c_d_id);
                            LOG.info(migration);
                            stmt.executeUpdate(migration);
                            Thread.sleep(300);
                            if (!flag) break;
                        }
                        if (!flag) break;
                    }
                }
                
                if (DBWorkload.BACKGROUND_THREAD.equals("join")) {
                    int o_start = 0;
                    for (int ol_w_id = 5; ol_w_id >= 3; ol_w_id--) {
                        if (ol_w_id == 3) {
                            o_start = 1500;
                        }
                        for (int ol_d_id = 1; ol_d_id <= 10; ol_d_id++) {
                            int prev = o_start;
                            for (int ol_o_id = prev + 300; ol_o_id <= 3000; ol_o_id += 300) {
                                String migration = MessageFormat.format(migrationFmt, ol_w_id, ol_d_id, prev, ol_o_id);
                                LOG.info(migration);
                                stmt.executeUpdate(migration);
                                prev = ol_o_id;
                                Thread.sleep(100);
                                if (!flag) break;
                            }
                            if (!flag) break;
                        }
                        if (!flag) break;
                    }                    
                }
            }
            stmt.close();
            c.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
