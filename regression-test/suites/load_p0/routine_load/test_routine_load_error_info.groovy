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

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.ProducerConfig

suite("test_routine_load_error_info","nonConcurrent") {
    def kafkaCsvTpoics = [
                  "test_error_info",
                ]

    String enabled = context.config.otherConfigs.get("enableKafkaTest")
    String kafka_port = context.config.otherConfigs.get("kafka_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    def kafka_broker = "${externalEnvIp}:${kafka_port}"

    // send data to kafka 
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        def props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "${kafka_broker}".toString())
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        // add timeout config
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000")  
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")

        // check conenction
        def verifyKafkaConnection = { prod ->
            try {
                logger.info("=====try to connect Kafka========")
                def partitions = prod.partitionsFor("__connection_verification_topic")
                return partitions != null
            } catch (Exception e) {
                throw new Exception("Kafka connect fail: ${e.message}".toString())
            }
        }
        // Create kafka producer
        def producer = new KafkaProducer<>(props)
        try {
            logger.info("Kafka connecting: ${kafka_broker}")
            if (!verifyKafkaConnection(producer)) {
                throw new Exception("can't get any kafka info")
            }
        } catch (Exception e) {
            logger.error("FATAL: " + e.getMessage())
            producer.close()
            throw e  
        }
        logger.info("Kafka connect success")
        for (String kafkaCsvTopic in kafkaCsvTpoics) {
            def txt = new File("""${context.file.parent}/data/${kafkaCsvTopic}.csv""").text
            def lines = txt.readLines()
            lines.each { line ->
                logger.info("=====${line}========")
                def record = new ProducerRecord<>(kafkaCsvTopic, null, line)
                producer.send(record)
            }
        }
    }

    def createTable = {tableName ->
        sql """
            DROP TABLE IF EXISTS ${tableName}
        """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName}
            (
                k00 INT             NOT NULL,
                k01 DATE            NOT NULL,
                k02 BOOLEAN         NULL,
                k03 TINYINT         NULL,
                k04 SMALLINT        NULL,
                k05 INT             NULL,
                k06 BIGINT          NULL,
                k07 LARGEINT        NULL,
                k08 FLOAT           NULL,
                k09 DOUBLE          NULL,
                k10 DECIMAL(9,1)    NULL,
                k11 DECIMALV3(9,1)  NULL,
                k12 DATETIME        NULL,
                k13 DATEV2          NULL,
                k14 DATETIMEV2      NULL,
                k15 CHAR            NULL,
                k16 VARCHAR         NULL,
                k17 STRING          NULL,
                k18 JSON            NULL,
                kd01 BOOLEAN         NOT NULL DEFAULT "TRUE",
                kd02 TINYINT         NOT NULL DEFAULT "1",
                kd03 SMALLINT        NOT NULL DEFAULT "2",
                kd04 INT             NOT NULL DEFAULT "3",
                kd05 BIGINT          NOT NULL DEFAULT "4",
                kd06 LARGEINT        NOT NULL DEFAULT "5",
                kd07 FLOAT           NOT NULL DEFAULT "6.0",
                kd08 DOUBLE          NOT NULL DEFAULT "7.0",
                kd09 DECIMAL         NOT NULL DEFAULT "888888888",
                kd10 DECIMALV3       NOT NULL DEFAULT "999999999",
                kd11 DATE            NOT NULL DEFAULT "2023-08-24",
                kd12 DATETIME        NOT NULL DEFAULT "2023-08-24 12:00:00",
                kd13 DATEV2          NOT NULL DEFAULT "2023-08-24",
                kd14 DATETIMEV2      NOT NULL DEFAULT "2023-08-24 12:00:00",
                kd15 CHAR(255)       NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
                kd16 VARCHAR(300)    NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
                kd17 STRING          NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
                kd18 JSON            NULL,
                
                INDEX idx_inverted_k104 (`k05`) USING INVERTED,
                INDEX idx_inverted_k110 (`k11`) USING INVERTED,
                INDEX idx_inverted_k113 (`k13`) USING INVERTED,
                INDEX idx_inverted_k114 (`k14`) USING INVERTED,
                INDEX idx_inverted_k117 (`k17`) USING INVERTED PROPERTIES("parser" = "english"),
                INDEX idx_ngrambf_k115 (`k15`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256"),
                INDEX idx_ngrambf_k116 (`k16`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256"),
                INDEX idx_ngrambf_k117 (`k17`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256"),

                INDEX idx_bitmap_k104 (`k02`) USING BITMAP,
                INDEX idx_bitmap_k110 (`kd01`) USING BITMAP
                
            )
            DUPLICATE KEY(k00)
            PARTITION BY RANGE(k01)
            (
                PARTITION p1 VALUES [('2023-08-01'), ('2023-08-11')),
                PARTITION p2 VALUES [('2023-08-11'), ('2023-08-21')),
                PARTITION p3 VALUES [('2023-08-21'), ('2023-09-01'))
            )
            DISTRIBUTED BY HASH(k00) BUCKETS 32
            PROPERTIES (
                "bloom_filter_columns"="k05",
                "replication_num" = "1"
            );
        """
    }

    def createJob = {jobName, tableName, kafkaTopic ->
        sql """
        CREATE ROUTINE LOAD ${jobName} on ${tableName}
                COLUMNS(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18),
                COLUMNS TERMINATED BY "|"
                PROPERTIES
                (
                    "max_batch_interval" = "5",
                    "max_batch_rows" = "300000",
                    "max_batch_size" = "209715200"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaTopic}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
        """
    }

    // case 1: task failed
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        // create table
        def jobName = "test_error_info"
        def tableName = "test_routine_error_info"
        try {
            createTable(tableName)
            sql "sync"
            GetDebugPoint().enableDebugPointForAllBEs("BetaRowsetWriter._check_segment_number_limit_too_many_segments")
            createJob(jobName, tableName, kafkaCsvTpoics[0])
            sql "sync"

            // check error info
            def count = 0
            while (true) {
                def res = sql "show routine load for ${jobName}"
                log.info("show routine load: ${res[0].toString()}".toString())
                log.info("other msg: ${res[0][19].toString()}".toString())
                if (res[0][19].toString() != "") {
                    assertTrue(res[0][19].toString().contains("too many segments in rowset"))
                    break;
                }
                count++
                if (count > 60) {
                    assertEquals(1, 2)
                    break;
                }
                sleep(1000)
            }
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("BetaRowsetWriter._check_segment_number_limit_too_many_segments")
            sql "stop routine load for ${jobName}"
            sql "DROP TABLE IF EXISTS ${tableName}"
        }
    }

    // case 2: reschedule job
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        def jobName = "test_error_info"
        def tableName = "test_routine_error_info"
        try {
            createTable(tableName)
            sql "sync"
            createJob(jobName, tableName, "invalid_job")
            sql "sync"

            // check error info
            def count = 0
            while (true) {
                def res = sql "show routine load for ${jobName}"
                log.info("show routine load: ${res[0].toString()}".toString())
                log.info("reason: ${res[0][17].toString()}".toString())
                if (res[0][17].toString() != "") {
                    assertTrue(res[0][17].toString().contains("may be Kafka properties set in job is error or no partition in this topic that should check Kafka"))
                    break;
                }
                count++
                if (count > 60) {
                    assertEquals(1, 2)
                    break;
                }
                sleep(1000)
            }
        } finally {
            sql "stop routine load for ${jobName}"
            sql "DROP TABLE IF EXISTS ${tableName}"
        }
    }

    // case 3: memory limit
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        def jobName = "test_memory_limit_error_info"
        def tableName = "test_routine_memory_limit_error_info"
        
        try {
            createTable(tableName)
            sql "sync"
            GetDebugPoint().enableDebugPointForAllBEs("RoutineLoadTaskExecutor.submit_task.memory_limit")
            createJob(jobName, tableName, kafkaCsvTpoics[0])
            sql "sync"

            // check error info
            def count = 0
            while (true) {
                def res = sql "show routine load for ${jobName}"
                log.info("show routine load: ${res[0].toString()}".toString())
                log.info("other msg: ${res[0][19].toString()}".toString())
                if (res[0][19].toString() != "") {
                    assertTrue(res[0][19].toString().contains("reach memory limit"))
                    break;
                }
                count++
                if (count > 60) {
                    assertEquals(1, 2)
                    break;
                }
                sleep(1000)
            }
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("RoutineLoadTaskExecutor.submit_task.memory_limit")
            sql "stop routine load for ${jobName}"
            sql "DROP TABLE IF EXISTS ${tableName}"
        }
    }
}