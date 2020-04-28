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

public class BgThread extends Thread {

    private static final Logger LOG = Logger.getLogger(BgThread.class);
    private volatile boolean flag = true;

    private Connection conn = null;
    private Statement stmt = null;

    private String migrationFmt =
        " insert into customer_proj(" +
        " c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, " +
        " c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, " +
        " c_street_1, c_city, c_state, c_zip, c_data) " +
        " (select " +
        " c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, " +
        " c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, " +
        " c_street_1, c_city, c_state, c_zip, c_data " +
        " from customer " +
        " where c_w_id = {0,number,#} " +
        " and c_d_id = {1,number,#}) ";
        // " on conflict (c_w_id,c_d_id,c_id) do nothing;";

    public BgThread(String name) {
        super(name); 
    }

    public void stopRunning() {
        flag = false;
    }

    public synchronized void run() {
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
            // TODO: do we need a bloom filter in here?
            for (int c_w_id = 1; c_w_id <= 50; c_w_id++) {
                for (int c_d_id = 1; c_d_id <= 10; c_d_id++) {
                    // <= 3000 tuples will be migrated each time
                    String migration = MessageFormat.format(migrationFmt, c_w_id, c_d_id);
                    LOG.info(migration);
                    c.setAutoCommit(false);
                    stmt.executeUpdate(migration);
                    c.commit();
                    Thread.sleep(400);
                    if (!flag) break;
                }
                if (!flag) break;
            }
            stmt.close();
            c.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
