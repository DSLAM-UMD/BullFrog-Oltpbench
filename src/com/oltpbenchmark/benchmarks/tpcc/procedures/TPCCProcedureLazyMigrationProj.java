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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;

public abstract class TPCCProcedureLazyMigrationProj extends TPCCProcedure {

    public ThreadLocal<Number> t_c_w_id = new ThreadLocal<Number>();
    public ThreadLocal<Number> t_c_d_id = new ThreadLocal<Number>();
    public ThreadLocal<Number> t_c_id = new ThreadLocal<Number>();

}
