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

import org.junit.Assert;

suite("test_show_backend_auth","p0,auth_call") {
    String user = 'test_show_backend_auth_user'
    String pwd = 'C123_567p'

    try_sql("DROP USER ${user}")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${user}""";
    }
    sql """grant select_priv on regression_test to ${user}"""
    sql """grant select_priv on internal.information_schema.* to ${user}"""
    def show_grants_result = sql """show grants for ${user}"""
    logger.info("show grants result: " + show_grants_result)
    sql """revoke select_priv on internal.information_schema.* from ${user}"""

    connect(user, "${pwd}", context.config.jdbcUrl) {
        
        try {
            def show_result = sql """SHOW BACKENDS"""
            logger.info("show_result: " + show_result)
        } catch (Exception e) {
            logger.info("show_result: " + e)
            e.printStackTrace()
        }
        test {
            sql """SHOW BACKENDS"""
            exception "denied"
        }
    }
    sql """grant select_priv on internal.information_schema.* to ${user}"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        def res = sql """SHOW BACKENDS"""
        assertTrue(res.size() > 0)
    }

    try_sql("DROP USER ${user}")
}
