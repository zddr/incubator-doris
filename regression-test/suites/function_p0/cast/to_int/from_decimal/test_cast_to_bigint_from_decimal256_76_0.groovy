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


suite("test_cast_to_bigint_from_decimal256_76_0") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set enable_decimal256 = true;"
    sql "drop table if exists test_cast_to_bigint_from_decimal256_76_0_0_nullable;"
    sql "create table test_cast_to_bigint_from_decimal256_76_0_0_nullable(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_bigint_from_decimal256_76_0_0_nullable values (0, "0"),(1, "1"),(2, "9"),(3, "9223372036854775807"),(4, "9223372036854775806"),(5, "-9223372036854775808"),(6, "-9223372036854775807")
      ,(7, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as bigint) from test_cast_to_bigint_from_decimal256_76_0_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as bigint) from test_cast_to_bigint_from_decimal256_76_0_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_bigint_from_decimal256_76_0_0_not_nullable;"
    sql "create table test_cast_to_bigint_from_decimal256_76_0_0_not_nullable(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_bigint_from_decimal256_76_0_0_not_nullable values (0, "0"),(1, "1"),(2, "9"),(3, "9223372036854775807"),(4, "9223372036854775806"),(5, "-9223372036854775808"),(6, "-9223372036854775807");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as bigint) from test_cast_to_bigint_from_decimal256_76_0_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as bigint) from test_cast_to_bigint_from_decimal256_76_0_0_not_nullable order by 1;'

}