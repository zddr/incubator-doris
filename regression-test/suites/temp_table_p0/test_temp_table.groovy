// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite('test_temp_table', 'p0') {
    // temporary table with dynamic partitions and colocate group
    sql """
        CREATE TEMPORARY TABLE temp_table_with_dyncmic_partition
        (
            k1 DATE
        )
        PARTITION BY RANGE(k1) ()
        DISTRIBUTED BY HASH(k1)
        PROPERTIES
        (
            "replication_allocation" = "tag.location.default: 1",
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "DAY",
            "dynamic_partition.start" = "-7",
            "dynamic_partition.end" = "3",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "10",
            "colocate_with" = "colocate_group1"
        );
        """
    def select_partitions1 = sql "show partitions from temp_table_with_dyncmic_partition"
    assertEquals(select_partitions1.size(), 4)

    try {
        sql "CREATE MATERIALIZED VIEW mv_mtmv1 as select k1 from temp_table_with_dyncmic_partition"
        throw new IllegalStateException("Should throw error")
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("do not support create materialized view on temporary table"), ex.getMessage())
    }
    def show_create_mv1 = sql "show create materialized view mv_mtmv1 on temp_table_with_dyncmic_partition"
    assertEquals(show_create_mv1.size(), 0)

    try {
        sql """
            CREATE MATERIALIZED VIEW mtmv_2 REFRESH COMPLETE ON SCHEDULE EVERY 100 hour
            PROPERTIES ('replication_num' = '1') 
            AS SELECT * FROM temp_table_with_dyncmic_partition;
            """

        throw new IllegalStateException("Should throw error")
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("do not support create materialized view on temporary table"), ex.getMessage())
    }


    // clean
    sql "use regression_test_temp_table_p0"
    sql "DROP USER IF EXISTS temp_table_test_user"
    sql """drop table if exists t_test_table_with_data"""
    sql """drop database if exists regression_test_temp_table_db2"""
}