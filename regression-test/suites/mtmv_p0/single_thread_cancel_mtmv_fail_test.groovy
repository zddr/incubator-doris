suite("single_thread_cancel_mtmv_fail_test") {

    def waitingMTMVTaskFinishedCur = { def jobName ->
        Thread.sleep(2000)
        String showTasks = "select TaskId,JobId,JobName,MvId,Status from tasks('type'='mv') where JobName = '${jobName}' order by CreateTime ASC"
        String status = "NULL"
        List<List<Object>> result
        long startTime = System.currentTimeMillis()
        long timeoutTimestamp = startTime + 30 * 60 * 1000 // 5 min
        do {
            result = sql(showTasks)
            logger.info("result: " + result.toString())
            if (!result.isEmpty()) {
                status = result.last().get(4)
            }
            logger.info("The state of ${showTasks} is ${status}")
            sleep(10 * 1000)
        } while (timeoutTimestamp > System.currentTimeMillis() && (status == 'PENDING' || status == 'RUNNING' || status == 'NULL'))
        if (status != "SUCCESS") {
            logger.info("status is not success")
            return false
        }
        assertEquals("SUCCESS", status)
        return true
    }

    def src_database_name = context.config.getDbNameByFile(context.file)
    def table_name1 = "lineitem"
    def table_name2 = "orders"



    def mv_name = "single_thread_cancel_mtmv_fail_mtmv"

    for (int num = 0; num < 100; num++) {
        sql """drop MATERIALIZED VIEW if exists ${mv_name};"""
        sql """
        CREATE MATERIALIZED VIEW ${mv_name}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') AS
        SELECT 
            li.l_orderkey,
            li.l_quantity,
            li.l_extendedprice,
            o.o_orderkey,
            o.o_totalprice,
            (li.l_extendedprice * (1 - li.l_discount) * (1 + li.l_tax)) + 
            (SELECT AVG(li2.l_extendedprice * (1 - li2.l_discount) * (1 + li2.l_tax)) 
             FROM lineitem li2) +
            (SELECT AVG(o2.o_totalprice) 
             FROM orders o2),
            li.l_extendedprice * li.l_tax,
            CASE 
                WHEN o.o_orderpriority = '1-URGENT' THEN 1.5
                WHEN o.o_orderpriority = '2-HIGH' THEN 1.2
                WHEN o.o_orderpriority = '3-MEDIUM' THEN 1.0
                WHEN o.o_orderpriority = '4-LOW' THEN 0.8
                ELSE 0.5
            END * 
            (SELECT AVG(o3.o_shippriority) FROM orders o3)
        FROM 
            lineitem li
        JOIN 
            orders o ON li.l_orderkey = o.o_orderkey
        WHERE 
            li.l_quantity > (SELECT AVG(l_quantity) FROM lineitem)
            AND 
            o.o_totalprice > (SELECT AVG(o_totalprice) FROM orders)
        """

        sleep(5 * 1000)
        def job_name = getJobName(src_database_name, mv_name)
        def task_id = sql """select * from tasks("type"="mv") where JobName="${job_name}";"""
        sql """CANCEL MATERIALIZED VIEW TASK ${task_id[0][0]} ON ${mv_name}"""

        sql """refresh MATERIALIZED VIEW ${mv_name} COMPLETE"""

        def task_list = sql """select * from tasks("type"="mv") where JobName="${job_name}" order by StartTime;"""
        logger.info("task_list:" + task_list)
        sleep(60 * 1000)
        task_list = sql """select * from tasks("type"="mv") where JobName="${job_name}" order by StartTime;"""
        logger.info("task_list:" + task_list)

        assertTrue(task_list.size() == 2)
        // assertTrue(task_list[1][7] == "FAILED" || task_list[1][7] == "SUCCESS")
        if (task_list[1][7] == "FAIL") {
            assertTrue(task_list[1][8].indexOf("Not allowed") != -1)
        }
    }



}