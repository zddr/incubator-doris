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


suite("test_cast_to_decimal128i_38_1_from_decimal64") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal_38_1_from_decimal_10_0_0_nullable;"
    sql "create table test_cast_to_decimal_38_1_from_decimal_10_0_0_nullable(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimal_10_0_0_nullable values (0, "0"),(1, "999999999"),(2, "9000000000"),(3, "9000000001"),(4, "9999999998"),(5, "9999999999")
      ,(6, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_10_0_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_10_0_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimal_10_0_0_not_nullable;"
    sql "create table test_cast_to_decimal_38_1_from_decimal_10_0_0_not_nullable(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimal_10_0_0_not_nullable values (0, "0"),(1, "999999999"),(2, "9000000000"),(3, "9000000001"),(4, "9999999998"),(5, "9999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_10_0_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_10_0_0_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimal_10_1_1_nullable;"
    sql "create table test_cast_to_decimal_38_1_from_decimal_10_1_1_nullable(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimal_10_1_1_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "99999999.0"),(5, "99999999.1"),(6, "99999999.8"),(7, "99999999.9"),(8, "900000000.0"),(9, "900000000.1"),(10, "900000000.8"),(11, "900000000.9"),(12, "900000001.0"),(13, "900000001.1"),(14, "900000001.8"),(15, "900000001.9"),(16, "999999998.0"),(17, "999999998.1"),(18, "999999998.8"),(19, "999999998.9"),
      (20, "999999999.0"),(21, "999999999.1"),(22, "999999999.8"),(23, "999999999.9")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_10_1_1_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_10_1_1_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimal_10_1_1_not_nullable;"
    sql "create table test_cast_to_decimal_38_1_from_decimal_10_1_1_not_nullable(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimal_10_1_1_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "99999999.0"),(5, "99999999.1"),(6, "99999999.8"),(7, "99999999.9"),(8, "900000000.0"),(9, "900000000.1"),(10, "900000000.8"),(11, "900000000.9"),(12, "900000001.0"),(13, "900000001.1"),(14, "900000001.8"),(15, "900000001.9"),(16, "999999998.0"),(17, "999999998.1"),(18, "999999998.8"),(19, "999999998.9"),
      (20, "999999999.0"),(21, "999999999.1"),(22, "999999999.8"),(23, "999999999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_10_1_1_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_10_1_1_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimal_10_9_2_nullable;"
    sql "create table test_cast_to_decimal_38_1_from_decimal_10_9_2_nullable(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimal_10_9_2_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.099999999"),(4, "0.900000000"),(5, "0.900000001"),(6, "0.999999998"),(7, "0.999999999"),(8, "8.000000000"),(9, "8.000000001"),(10, "8.000000009"),(11, "8.099999999"),(12, "8.900000000"),(13, "8.900000001"),(14, "8.999999998"),(15, "8.999999999"),(16, "9.000000000"),(17, "9.000000001"),(18, "9.000000009"),(19, "9.099999999"),
      (20, "9.900000000"),(21, "9.900000001"),(22, "9.999999998"),(23, "9.999999999")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_10_9_2_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_10_9_2_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimal_10_9_2_not_nullable;"
    sql "create table test_cast_to_decimal_38_1_from_decimal_10_9_2_not_nullable(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimal_10_9_2_not_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.099999999"),(4, "0.900000000"),(5, "0.900000001"),(6, "0.999999998"),(7, "0.999999999"),(8, "8.000000000"),(9, "8.000000001"),(10, "8.000000009"),(11, "8.099999999"),(12, "8.900000000"),(13, "8.900000001"),(14, "8.999999998"),(15, "8.999999999"),(16, "9.000000000"),(17, "9.000000001"),(18, "9.000000009"),(19, "9.099999999"),
      (20, "9.900000000"),(21, "9.900000001"),(22, "9.999999998"),(23, "9.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_10_9_2_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_10_9_2_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimal_10_10_3_nullable;"
    sql "create table test_cast_to_decimal_38_1_from_decimal_10_10_3_nullable(f1 int, f2 decimalv3(10, 10)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimal_10_10_3_nullable values (0, "0.0000000000"),(1, "0.0000000001"),(2, "0.0000000009"),(3, "0.0999999999"),(4, "0.9000000000"),(5, "0.9000000001"),(6, "0.9999999998"),(7, "0.9999999999")
      ,(8, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_10_10_3_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_10_10_3_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimal_10_10_3_not_nullable;"
    sql "create table test_cast_to_decimal_38_1_from_decimal_10_10_3_not_nullable(f1 int, f2 decimalv3(10, 10)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimal_10_10_3_not_nullable values (0, "0.0000000000"),(1, "0.0000000001"),(2, "0.0000000009"),(3, "0.0999999999"),(4, "0.9000000000"),(5, "0.9000000001"),(6, "0.9999999998"),(7, "0.9999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_10_10_3_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_10_10_3_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimal_18_0_4_nullable;"
    sql "create table test_cast_to_decimal_38_1_from_decimal_18_0_4_nullable(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimal_18_0_4_nullable values (0, "0"),(1, "99999999999999999"),(2, "900000000000000000"),(3, "900000000000000001"),(4, "999999999999999998"),(5, "999999999999999999")
      ,(6, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_18_0_4_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_18_0_4_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimal_18_0_4_not_nullable;"
    sql "create table test_cast_to_decimal_38_1_from_decimal_18_0_4_not_nullable(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimal_18_0_4_not_nullable values (0, "0"),(1, "99999999999999999"),(2, "900000000000000000"),(3, "900000000000000001"),(4, "999999999999999998"),(5, "999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_18_0_4_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_18_0_4_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimal_18_1_5_nullable;"
    sql "create table test_cast_to_decimal_38_1_from_decimal_18_1_5_nullable(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimal_18_1_5_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "9999999999999999.0"),(5, "9999999999999999.1"),(6, "9999999999999999.8"),(7, "9999999999999999.9"),(8, "90000000000000000.0"),(9, "90000000000000000.1"),(10, "90000000000000000.8"),(11, "90000000000000000.9"),(12, "90000000000000001.0"),(13, "90000000000000001.1"),(14, "90000000000000001.8"),(15, "90000000000000001.9"),(16, "99999999999999998.0"),(17, "99999999999999998.1"),(18, "99999999999999998.8"),(19, "99999999999999998.9"),
      (20, "99999999999999999.0"),(21, "99999999999999999.1"),(22, "99999999999999999.8"),(23, "99999999999999999.9")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_18_1_5_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_18_1_5_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimal_18_1_5_not_nullable;"
    sql "create table test_cast_to_decimal_38_1_from_decimal_18_1_5_not_nullable(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimal_18_1_5_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "9999999999999999.0"),(5, "9999999999999999.1"),(6, "9999999999999999.8"),(7, "9999999999999999.9"),(8, "90000000000000000.0"),(9, "90000000000000000.1"),(10, "90000000000000000.8"),(11, "90000000000000000.9"),(12, "90000000000000001.0"),(13, "90000000000000001.1"),(14, "90000000000000001.8"),(15, "90000000000000001.9"),(16, "99999999999999998.0"),(17, "99999999999999998.1"),(18, "99999999999999998.8"),(19, "99999999999999998.9"),
      (20, "99999999999999999.0"),(21, "99999999999999999.1"),(22, "99999999999999999.8"),(23, "99999999999999999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_18_1_5_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_18_1_5_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimal_18_17_6_nullable;"
    sql "create table test_cast_to_decimal_38_1_from_decimal_18_17_6_nullable(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimal_18_17_6_nullable values (0, "0.00000000000000000"),(1, "0.00000000000000001"),(2, "0.00000000000000009"),(3, "0.09999999999999999"),(4, "0.90000000000000000"),(5, "0.90000000000000001"),(6, "0.99999999999999998"),(7, "0.99999999999999999"),(8, "8.00000000000000000"),(9, "8.00000000000000001"),(10, "8.00000000000000009"),(11, "8.09999999999999999"),(12, "8.90000000000000000"),(13, "8.90000000000000001"),(14, "8.99999999999999998"),(15, "8.99999999999999999"),(16, "9.00000000000000000"),(17, "9.00000000000000001"),(18, "9.00000000000000009"),(19, "9.09999999999999999"),
      (20, "9.90000000000000000"),(21, "9.90000000000000001"),(22, "9.99999999999999998"),(23, "9.99999999999999999")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_18_17_6_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_18_17_6_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimal_18_17_6_not_nullable;"
    sql "create table test_cast_to_decimal_38_1_from_decimal_18_17_6_not_nullable(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimal_18_17_6_not_nullable values (0, "0.00000000000000000"),(1, "0.00000000000000001"),(2, "0.00000000000000009"),(3, "0.09999999999999999"),(4, "0.90000000000000000"),(5, "0.90000000000000001"),(6, "0.99999999999999998"),(7, "0.99999999999999999"),(8, "8.00000000000000000"),(9, "8.00000000000000001"),(10, "8.00000000000000009"),(11, "8.09999999999999999"),(12, "8.90000000000000000"),(13, "8.90000000000000001"),(14, "8.99999999999999998"),(15, "8.99999999999999999"),(16, "9.00000000000000000"),(17, "9.00000000000000001"),(18, "9.00000000000000009"),(19, "9.09999999999999999"),
      (20, "9.90000000000000000"),(21, "9.90000000000000001"),(22, "9.99999999999999998"),(23, "9.99999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_18_17_6_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_18_17_6_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimal_18_18_7_nullable;"
    sql "create table test_cast_to_decimal_38_1_from_decimal_18_18_7_nullable(f1 int, f2 decimalv3(18, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimal_18_18_7_nullable values (0, "0.000000000000000000"),(1, "0.000000000000000001"),(2, "0.000000000000000009"),(3, "0.099999999999999999"),(4, "0.900000000000000000"),(5, "0.900000000000000001"),(6, "0.999999999999999998"),(7, "0.999999999999999999")
      ,(8, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_18_18_7_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_18_18_7_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimal_18_18_7_not_nullable;"
    sql "create table test_cast_to_decimal_38_1_from_decimal_18_18_7_not_nullable(f1 int, f2 decimalv3(18, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimal_18_18_7_not_nullable values (0, "0.000000000000000000"),(1, "0.000000000000000001"),(2, "0.000000000000000009"),(3, "0.099999999999999999"),(4, "0.900000000000000000"),(5, "0.900000000000000001"),(6, "0.999999999999999998"),(7, "0.999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_18_18_7_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_18_18_7_not_nullable order by 1;'

}