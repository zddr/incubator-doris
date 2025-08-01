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

package org.apache.doris.metric;

import org.apache.doris.alter.Alter;
import org.apache.doris.alter.AlterJobV2.JobType;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.Pair;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.Version;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.loadv2.JobState;
import org.apache.doris.load.loadv2.LoadManager;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.load.routineload.RoutineLoadManager;
import org.apache.doris.metric.Metric.MetricUnit;
import org.apache.doris.monitor.jvm.JvmService;
import org.apache.doris.monitor.jvm.JvmStats;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.transaction.TransactionStatus;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

public final class MetricRepo {
    private static final Logger LOG = LogManager.getLogger(MetricRepo.class);

    // METRIC_REGISTER is only used for histogram metrics
    public static final MetricRegistry METRIC_REGISTER = new MetricRegistry();
    public static final DorisMetricRegistry DORIS_METRIC_REGISTER = new DorisMetricRegistry();

    public static volatile boolean isInit = false;
    public static final SystemMetrics SYSTEM_METRICS = new SystemMetrics();

    public static final String TABLET_NUM = "tablet_num";
    public static final String TABLET_MAX_COMPACTION_SCORE = "tablet_max_compaction_score";
    public static final String CLOUD_TAG = "cloud";

    public static LongCounterMetric COUNTER_REQUEST_ALL;
    public static LongCounterMetric COUNTER_QUERY_ALL;
    public static LongCounterMetric COUNTER_QUERY_ERR;
    public static LongCounterMetric COUNTER_QUERY_SLOW;
    public static LongCounterMetric COUNTER_QUERY_TABLE;
    public static LongCounterMetric COUNTER_QUERY_OLAP_TABLE;
    public static LongCounterMetric COUNTER_QUERY_HIVE_TABLE;

    public static LongCounterMetric HTTP_COUNTER_COPY_INFO_UPLOAD_REQUEST;
    public static LongCounterMetric HTTP_COUNTER_COPY_INFO_UPLOAD_ERR;
    public static LongCounterMetric HTTP_COUNTER_COPY_INFO_QUERY_REQUEST;
    public static LongCounterMetric HTTP_COUNTER_COPY_INFO_QUERY_ERR;

    public static AutoMappedMetric<LongCounterMetric> USER_COUNTER_QUERY_ALL;
    public static AutoMappedMetric<LongCounterMetric> USER_COUNTER_QUERY_ERR;
    public static Histogram HISTO_QUERY_LATENCY;
    public static AutoMappedMetric<Histogram> USER_HISTO_QUERY_LATENCY;
    public static AutoMappedMetric<GaugeMetricImpl<Long>> USER_GAUGE_QUERY_INSTANCE_NUM;
    public static AutoMappedMetric<GaugeMetricImpl<Integer>> USER_GAUGE_CONNECTIONS;
    public static AutoMappedMetric<LongCounterMetric> USER_COUNTER_QUERY_INSTANCE_BEGIN;
    public static AutoMappedMetric<LongCounterMetric> BE_COUNTER_QUERY_RPC_ALL;
    public static AutoMappedMetric<LongCounterMetric> BE_COUNTER_QUERY_RPC_FAILED;
    public static AutoMappedMetric<LongCounterMetric> BE_COUNTER_QUERY_RPC_SIZE;

    public static LongCounterMetric COUNTER_CACHE_ADDED_SQL;
    public static LongCounterMetric COUNTER_CACHE_ADDED_PARTITION;
    public static LongCounterMetric COUNTER_CACHE_HIT_SQL;
    public static LongCounterMetric COUNTER_CACHE_HIT_PARTITION;

    public static LongCounterMetric COUNTER_UPDATE_TABLET_STAT_FAILED;

    public static LongCounterMetric COUNTER_EDIT_LOG_WRITE;
    public static LongCounterMetric COUNTER_EDIT_LOG_READ;
    public static LongCounterMetric COUNTER_EDIT_LOG_CURRENT;
    public static LongCounterMetric COUNTER_EDIT_LOG_SIZE_BYTES;
    public static LongCounterMetric COUNTER_CURRENT_EDIT_LOG_SIZE_BYTES;
    public static LongCounterMetric COUNTER_EDIT_LOG_CLEAN_SUCCESS;
    public static LongCounterMetric COUNTER_EDIT_LOG_CLEAN_FAILED;
    public static LongCounterMetric COUNTER_LARGE_EDIT_LOG;

    public static Histogram HISTO_EDIT_LOG_WRITE_LATENCY;
    public static Histogram HISTO_JOURNAL_WRITE_LATENCY;
    public static Histogram HISTO_JOURNAL_BATCH_SIZE;
    public static Histogram HISTO_JOURNAL_BATCH_DATA_SIZE;
    public static Histogram HISTO_HTTP_COPY_INTO_UPLOAD_LATENCY;
    public static Histogram HISTO_HTTP_COPY_INTO_QUERY_LATENCY;

    public static LongCounterMetric COUNTER_IMAGE_WRITE_SUCCESS;
    public static LongCounterMetric COUNTER_IMAGE_WRITE_FAILED;
    public static LongCounterMetric COUNTER_IMAGE_PUSH_SUCCESS;
    public static LongCounterMetric COUNTER_IMAGE_PUSH_FAILED;
    public static LongCounterMetric COUNTER_IMAGE_CLEAN_SUCCESS;
    public static LongCounterMetric COUNTER_IMAGE_CLEAN_FAILED;

    public static LongCounterMetric COUNTER_TXN_REJECT;
    public static LongCounterMetric COUNTER_TXN_BEGIN;
    public static LongCounterMetric COUNTER_TXN_FAILED;
    public static LongCounterMetric COUNTER_TXN_SUCCESS;
    public static Histogram HISTO_TXN_EXEC_LATENCY;
    public static Histogram HISTO_TXN_PUBLISH_LATENCY;
    public static AutoMappedMetric<GaugeMetricImpl<Long>> DB_GAUGE_TXN_NUM;
    public static AutoMappedMetric<GaugeMetricImpl<Long>> DB_GAUGE_PUBLISH_TXN_NUM;

    public static LongCounterMetric COUNTER_ROUTINE_LOAD_ROWS;
    public static LongCounterMetric COUNTER_ROUTINE_LOAD_RECEIVED_BYTES;
    public static LongCounterMetric COUNTER_ROUTINE_LOAD_ERROR_ROWS;

    public static GaugeMetric<Long> GAUGE_ROUTINE_LOAD_PROGRESS;
    public static GaugeMetric<Long> GAUGE_ROUTINE_LOAD_LAG;
    public static GaugeMetric<Long> GAUGE_ROUTINE_LOAD_ABORT_TASK_NUM;
    public static LongCounterMetric COUNTER_ROUTINE_LOAD_GET_META_LANTENCY;
    public static LongCounterMetric COUNTER_ROUTINE_LOAD_GET_META_COUNT;
    public static LongCounterMetric COUNTER_ROUTINE_LOAD_GET_META_FAIL_COUNT;
    public static LongCounterMetric COUNTER_ROUTINE_LOAD_TASK_EXECUTE_TIME;
    public static LongCounterMetric COUNTER_ROUTINE_LOAD_TASK_EXECUTE_COUNT;

    public static LongCounterMetric COUNTER_HIT_SQL_BLOCK_RULE;

    public static AutoMappedMetric<LongCounterMetric> THRIFT_COUNTER_RPC_ALL;
    public static AutoMappedMetric<LongCounterMetric> THRIFT_COUNTER_RPC_LATENCY;

    // following metrics will be updated by metric calculator
    public static GaugeMetricImpl<Double> GAUGE_QUERY_PER_SECOND;
    public static GaugeMetricImpl<Double> GAUGE_REQUEST_PER_SECOND;
    public static GaugeMetricImpl<Double> GAUGE_QUERY_ERR_RATE;
    public static GaugeMetricImpl<Double> GAUGE_QUERY_SLOW_RATE;
    public static GaugeMetricImpl<Long> GAUGE_MAX_TABLET_COMPACTION_SCORE;

    public static Histogram HISTO_COMMIT_AND_PUBLISH_LATENCY;

    public static Histogram HISTO_GET_DELETE_BITMAP_UPDATE_LOCK_LATENCY;
    public static Histogram HISTO_GET_COMMIT_LOCK_LATENCY;
    public static Histogram HISTO_CALCULATE_DELETE_BITMAP_LATENCY;
    public static Histogram HISTO_COMMIT_TO_MS_LATENCY;

    // Catlaog/Database/Table num
    public static GaugeMetric<Integer> GAUGE_CATALOG_NUM;
    public static GaugeMetric<Integer> GAUGE_INTERNAL_DATABASE_NUM;
    public static GaugeMetric<Integer> GAUGE_INTERNAL_TABLE_NUM;
    // Table/Partition/Tablet DataSize
    public static GaugeMetricImpl<Long> GAUGE_MAX_TABLE_SIZE_BYTES;
    public static GaugeMetricImpl<Long> GAUGE_MAX_PARTITION_SIZE_BYTES;
    public static GaugeMetricImpl<Long> GAUGE_MAX_TABLET_SIZE_BYTES;
    public static GaugeMetricImpl<Long> GAUGE_MIN_TABLE_SIZE_BYTES;
    public static GaugeMetricImpl<Long> GAUGE_MIN_PARTITION_SIZE_BYTES;
    public static GaugeMetricImpl<Long> GAUGE_MIN_TABLET_SIZE_BYTES;
    public static GaugeMetricImpl<Long> GAUGE_AVG_TABLE_SIZE_BYTES;
    public static GaugeMetricImpl<Long> GAUGE_AVG_PARTITION_SIZE_BYTES;
    public static GaugeMetricImpl<Long> GAUGE_AVG_TABLET_SIZE_BYTES;

    // Agent task
    public static LongCounterMetric COUNTER_AGENT_TASK_REQUEST_TOTAL;
    public static AutoMappedMetric<LongCounterMetric> COUNTER_AGENT_TASK_TOTAL;
    public static AutoMappedMetric<LongCounterMetric> COUNTER_AGENT_TASK_RESEND_TOTAL;

    private static Map<Pair<EtlJobType, JobState>, Long> loadJobNum = Maps.newHashMap();

    private static final ScheduledThreadPoolExecutor metricTimer = ThreadPoolManager.newDaemonScheduledThreadPool(1,
            "metric-timer-pool", true);
    private static final MetricCalculator metricCalculator = new MetricCalculator();

    // init() should only be called after catalog is constructed.
    public static synchronized void init() {
        if (isInit) {
            return;
        }

        // version
        GaugeMetric<Long> feVersion = new GaugeMetric<Long>("version", MetricUnit.NOUNIT, "") {
            @Override
            public Long getValue() {
                try {
                    String verStr = Version.DORIS_BUILD_VERSION_MAJOR + "0" + Version.DORIS_BUILD_VERSION_MINOR + "0"
                            + Version.DORIS_BUILD_VERSION_PATCH;
                    if (Version.DORIS_BUILD_VERSION_HOTFIX > 0) {
                        verStr += ("0" + Version.DORIS_BUILD_VERSION_HOTFIX);
                    }
                    return Long.parseLong(verStr);
                } catch (Throwable t) {
                    LOG.warn("failed to init version metrics", t);
                    return 0L;
                }
            }
        };
        feVersion.addLabel(new MetricLabel("version", Version.DORIS_BUILD_VERSION));
        feVersion.addLabel(new MetricLabel("major", String.valueOf(Version.DORIS_BUILD_VERSION_MAJOR)));
        feVersion.addLabel(new MetricLabel("minor", String.valueOf(Version.DORIS_BUILD_VERSION_MINOR)));
        feVersion.addLabel(new MetricLabel("patch", String.valueOf(Version.DORIS_BUILD_VERSION_PATCH)));
        feVersion.addLabel(new MetricLabel("hotfix", String.valueOf(Version.DORIS_BUILD_VERSION_HOTFIX)));
        feVersion.addLabel(new MetricLabel("short_hash", Version.DORIS_BUILD_SHORT_HASH));
        DORIS_METRIC_REGISTER.addMetrics(feVersion);

        // load jobs
        for (EtlJobType jobType : EtlJobType.values()) {
            if (jobType == EtlJobType.UNKNOWN) {
                continue;
            }
            for (JobState state : JobState.values()) {
                GaugeMetric<Long> gauge = new GaugeMetric<Long>("job", MetricUnit.NOUNIT, "job statistics") {
                    @Override
                    public Long getValue() {
                        if (!Env.getCurrentEnv().isMaster()) {
                            return 0L;
                        }
                        return MetricRepo.getLoadJobNum(jobType, state);
                    }
                };
                gauge.addLabel(new MetricLabel("job", "load")).addLabel(new MetricLabel("type", jobType.name()))
                        .addLabel(new MetricLabel("state", state.name()));
                DORIS_METRIC_REGISTER.addMetrics(gauge);
            }
        }

        initRoutineLoadJobMetrics();

        // running alter job
        Alter alter = Env.getCurrentEnv().getAlterInstance();
        for (JobType jobType : JobType.values()) {
            if (jobType != JobType.SCHEMA_CHANGE && jobType != JobType.ROLLUP) {
                continue;
            }

            GaugeMetric<Long> gauge = new GaugeMetric<Long>("job", MetricUnit.NOUNIT, "job statistics") {
                @Override
                public Long getValue() {
                    if (!Env.getCurrentEnv().isMaster()) {
                        return 0L;
                    }
                    if (jobType == JobType.SCHEMA_CHANGE) {
                        return alter.getSchemaChangeHandler()
                                .getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.RUNNING);
                    } else {
                        return alter.getMaterializedViewHandler().getAlterJobV2Num(
                                org.apache.doris.alter.AlterJobV2.JobState.RUNNING);
                    }
                }
            };
            gauge.addLabel(new MetricLabel("job", "alter")).addLabel(new MetricLabel("type", jobType.name()))
                    .addLabel(new MetricLabel("state", "running"));
            DORIS_METRIC_REGISTER.addMetrics(gauge);
        }

        // capacity
        generateBackendsTabletMetrics();

        // connections
        USER_GAUGE_CONNECTIONS = addLabeledMetrics("user", () ->
                new GaugeMetricImpl<>("connection_total", MetricUnit.CONNECTIONS,
                        "total connections", 0));
        GaugeMetric<Integer> connections = new GaugeMetric<Integer>("connection_total",
                MetricUnit.CONNECTIONS, "total connections") {
            @Override
            public Integer getValue() {
                ExecuteEnv.getInstance().getScheduler().getUserConnectionMap()
                        .forEach((k, v) -> USER_GAUGE_CONNECTIONS.getOrAdd(k).setValue(v.get()));
                return ExecuteEnv.getInstance().getScheduler().getConnectionNum();
            }
        };
        DORIS_METRIC_REGISTER.addMetrics(connections);

        // journal id
        GaugeMetric<Long> maxJournalId = new GaugeMetric<Long>("max_journal_id", MetricUnit.NOUNIT,
                "max journal id of this frontends") {
            @Override
            public Long getValue() {
                EditLog editLog = Env.getCurrentEnv().getEditLog();
                if (editLog == null) {
                    return -1L;
                }
                return editLog.getMaxJournalId();
            }
        };
        DORIS_METRIC_REGISTER.addMetrics(maxJournalId);

        // scheduled tablet num
        GaugeMetric<Long> scheduledTabletNum = new GaugeMetric<Long>("scheduled_tablet_num", MetricUnit.NOUNIT,
                "number of tablets being scheduled") {
            @Override
            public Long getValue() {
                if (!Env.getCurrentEnv().isMaster()) {
                    return 0L;
                }
                return (long) Env.getCurrentEnv().getTabletScheduler().getTotalNum();
            }
        };
        DORIS_METRIC_REGISTER.addMetrics(scheduledTabletNum);

        // txn status
        for (TransactionStatus status : TransactionStatus.values()) {
            GaugeMetric<Long> gauge = new GaugeMetric<Long>("txn_status", MetricUnit.NOUNIT, "txn statistics") {
                @Override
                public Long getValue() {
                    if (!Env.getCurrentEnv().isMaster()) {
                        return 0L;
                    }
                    return Env.getCurrentGlobalTransactionMgr().getTxnNumByStatus(status);
                }
            };
            gauge.addLabel(new MetricLabel("type", status.name().toLowerCase()));
            DORIS_METRIC_REGISTER.addMetrics(gauge);
        }

        // qps, rps and error rate
        // these metrics should be set an init value, in case that metric calculator is not running
        GAUGE_QUERY_PER_SECOND = new GaugeMetricImpl<>("qps", MetricUnit.NOUNIT, "query per second", 0.0);
        DORIS_METRIC_REGISTER.addMetrics(GAUGE_QUERY_PER_SECOND);
        GAUGE_REQUEST_PER_SECOND = new GaugeMetricImpl<>("rps", MetricUnit.NOUNIT, "request per second", 0.0);
        DORIS_METRIC_REGISTER.addMetrics(GAUGE_REQUEST_PER_SECOND);
        GAUGE_QUERY_ERR_RATE = new GaugeMetricImpl<>("query_err_rate", MetricUnit.NOUNIT, "query error rate", 0.0);
        DORIS_METRIC_REGISTER.addMetrics(GAUGE_QUERY_ERR_RATE);
        GAUGE_QUERY_SLOW_RATE = new GaugeMetricImpl<>("query_slow_rate", MetricUnit.NOUNIT, "query slow rate", 0.0);
        DORIS_METRIC_REGISTER.addMetrics(GAUGE_QUERY_SLOW_RATE);
        GAUGE_MAX_TABLET_COMPACTION_SCORE = new GaugeMetricImpl<>("max_tablet_compaction_score", MetricUnit.NOUNIT,
                "max tablet compaction score of all backends", 0L);
        DORIS_METRIC_REGISTER.addMetrics(GAUGE_MAX_TABLET_COMPACTION_SCORE);

        // query
        COUNTER_REQUEST_ALL = new LongCounterMetric("request_total", MetricUnit.REQUESTS, "total request");
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_REQUEST_ALL);
        COUNTER_QUERY_ALL = new LongCounterMetric("query_total", MetricUnit.REQUESTS, "total query");
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_QUERY_ALL);
        COUNTER_QUERY_ERR = new LongCounterMetric("query_err", MetricUnit.REQUESTS, "total error query");
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_QUERY_ERR);
        COUNTER_QUERY_SLOW = new LongCounterMetric("query_slow", MetricUnit.REQUESTS, "total slow query");
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_QUERY_SLOW);
        COUNTER_QUERY_TABLE = new LongCounterMetric("query_table", MetricUnit.REQUESTS, "total query from table");
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_QUERY_TABLE);
        COUNTER_QUERY_OLAP_TABLE = new LongCounterMetric("query_olap_table", MetricUnit.REQUESTS,
                "total query from olap table");
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_QUERY_OLAP_TABLE);
        COUNTER_QUERY_HIVE_TABLE = new LongCounterMetric("query_hive_table", MetricUnit.REQUESTS,
                "total query from hive table");
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_QUERY_HIVE_TABLE);
        USER_COUNTER_QUERY_ALL = new AutoMappedMetric<>(name -> {
            LongCounterMetric userCountQueryAll = new LongCounterMetric("query_total", MetricUnit.REQUESTS,
                    "total query for single user");
            userCountQueryAll.addLabel(new MetricLabel("user", name));
            DORIS_METRIC_REGISTER.addMetrics(userCountQueryAll);
            return userCountQueryAll;
        });
        USER_COUNTER_QUERY_ERR = new AutoMappedMetric<>(name -> {
            LongCounterMetric userCountQueryErr = new LongCounterMetric("query_err", MetricUnit.REQUESTS,
                    "total error query for single user");
            userCountQueryErr.addLabel(new MetricLabel("user", name));
            DORIS_METRIC_REGISTER.addMetrics(userCountQueryErr);
            return userCountQueryErr;
        });
        HISTO_QUERY_LATENCY = METRIC_REGISTER.histogram(
                MetricRegistry.name("query", "latency", "ms"));
        USER_HISTO_QUERY_LATENCY = new AutoMappedMetric<>(name -> {
            String metricName = MetricRegistry.name("query", "latency", "ms", "user=" + name);
            return METRIC_REGISTER.histogram(metricName);
        });
        USER_COUNTER_QUERY_INSTANCE_BEGIN = addLabeledMetrics("user", () ->
                new LongCounterMetric("query_instance_begin", MetricUnit.NOUNIT,
                "number of query instance begin"));
        USER_GAUGE_QUERY_INSTANCE_NUM = addLabeledMetrics("user", () ->
                new GaugeMetricImpl<>("query_instance_num", MetricUnit.NOUNIT,
                "number of running query instances of current user", 0L));
        GaugeMetric<Long> queryInstanceNum = new GaugeMetric<Long>("query_instance_num",
                MetricUnit.NOUNIT, "number of query instances of all current users") {
            @Override
            public Long getValue() {
                QeProcessorImpl qe = ((QeProcessorImpl) QeProcessorImpl.INSTANCE);
                long totalInstanceNum = 0;
                for (Map.Entry<String, Integer> e : qe.getInstancesNumPerUser().entrySet()) {
                    long value = e.getValue() == null ? 0L : e.getValue().longValue();
                    totalInstanceNum += value;
                    USER_GAUGE_QUERY_INSTANCE_NUM.getOrAdd(e.getKey()).setValue(value);
                }
                return totalInstanceNum;
            }
        };
        DORIS_METRIC_REGISTER.addMetrics(queryInstanceNum);
        BE_COUNTER_QUERY_RPC_ALL = addLabeledMetrics("be", () ->
            new LongCounterMetric("query_rpc_total", MetricUnit.NOUNIT, ""));
        BE_COUNTER_QUERY_RPC_FAILED = addLabeledMetrics("be", () ->
            new LongCounterMetric("query_rpc_failed", MetricUnit.NOUNIT, ""));
        BE_COUNTER_QUERY_RPC_SIZE = addLabeledMetrics("be", () ->
            new LongCounterMetric("query_rpc_size", MetricUnit.BYTES, ""));

        // cache
        COUNTER_CACHE_ADDED_SQL = new LongCounterMetric("cache_added", MetricUnit.REQUESTS,
                "Number of SQL mode cache added");
        COUNTER_CACHE_ADDED_SQL.addLabel(new MetricLabel("type", "sql"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_CACHE_ADDED_SQL);
        COUNTER_CACHE_ADDED_PARTITION = new LongCounterMetric("cache_added", MetricUnit.REQUESTS,
                "Number of Partition mode cache added");
        COUNTER_CACHE_ADDED_PARTITION.addLabel(new MetricLabel("type", "partition"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_CACHE_ADDED_PARTITION);
        COUNTER_CACHE_HIT_SQL = new LongCounterMetric("cache_hit", MetricUnit.REQUESTS,
                "total hits query by sql model");
        COUNTER_CACHE_HIT_SQL.addLabel(new MetricLabel("type", "sql"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_CACHE_HIT_SQL);
        COUNTER_CACHE_HIT_PARTITION = new LongCounterMetric("cache_hit", MetricUnit.REQUESTS,
                "total hits query by partition model");
        COUNTER_CACHE_HIT_PARTITION.addLabel(new MetricLabel("type", "partition"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_CACHE_HIT_PARTITION);

        // edit log
        COUNTER_EDIT_LOG_WRITE = new LongCounterMetric("edit_log", MetricUnit.OPERATIONS,
                "counter of edit log write into bdbje");
        COUNTER_EDIT_LOG_WRITE.addLabel(new MetricLabel("type", "write"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_EDIT_LOG_WRITE);
        COUNTER_EDIT_LOG_READ = new LongCounterMetric("edit_log", MetricUnit.OPERATIONS,
                "counter of edit log read from bdbje");
        COUNTER_EDIT_LOG_READ.addLabel(new MetricLabel("type", "read"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_EDIT_LOG_READ);
        COUNTER_EDIT_LOG_CURRENT = new LongCounterMetric("edit_log", MetricUnit.OPERATIONS,
                "counter of current edit log in bdbje");
        COUNTER_EDIT_LOG_CURRENT.addLabel(new MetricLabel("type", "current"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_EDIT_LOG_CURRENT);
        COUNTER_EDIT_LOG_SIZE_BYTES = new LongCounterMetric("edit_log", MetricUnit.BYTES,
                "size of accumulated edit log");
        COUNTER_EDIT_LOG_SIZE_BYTES.addLabel(new MetricLabel("type", "accumulated_bytes"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_EDIT_LOG_SIZE_BYTES);
        COUNTER_CURRENT_EDIT_LOG_SIZE_BYTES = new LongCounterMetric("edit_log", MetricUnit.BYTES,
                "size of current edit log");
        COUNTER_CURRENT_EDIT_LOG_SIZE_BYTES.addLabel(new MetricLabel("type", "current_bytes"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_CURRENT_EDIT_LOG_SIZE_BYTES);

        COUNTER_LARGE_EDIT_LOG = new LongCounterMetric("edit_log", MetricUnit.OPERATIONS,
                "counter of large edit log write into bdbje");
        COUNTER_LARGE_EDIT_LOG.addLabel(new MetricLabel("type", "large_write"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_LARGE_EDIT_LOG);

        HISTO_EDIT_LOG_WRITE_LATENCY = METRIC_REGISTER.histogram(
                MetricRegistry.name("editlog", "write", "latency", "ms"));
        HISTO_JOURNAL_WRITE_LATENCY = METRIC_REGISTER.histogram(
                MetricRegistry.name("journal", "write", "latency", "ms"));
        HISTO_JOURNAL_BATCH_SIZE = METRIC_REGISTER.histogram(
                MetricRegistry.name("journal", "write", "batch_size"));
        HISTO_JOURNAL_BATCH_DATA_SIZE = METRIC_REGISTER.histogram(
                MetricRegistry.name("journal", "write", "batch_data_size"));

        // edit log clean
        COUNTER_EDIT_LOG_CLEAN_SUCCESS = new LongCounterMetric("edit_log_clean", MetricUnit.OPERATIONS,
            "counter of edit log succeed in cleaning");
        COUNTER_EDIT_LOG_CLEAN_SUCCESS.addLabel(new MetricLabel("type", "success"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_EDIT_LOG_CLEAN_SUCCESS);
        COUNTER_EDIT_LOG_CLEAN_FAILED = new LongCounterMetric("edit_log_clean", MetricUnit.OPERATIONS,
            "counter of edit log failed to clean");
        COUNTER_EDIT_LOG_CLEAN_FAILED.addLabel(new MetricLabel("type", "failed"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_EDIT_LOG_CLEAN_FAILED);

        // image generate
        COUNTER_IMAGE_WRITE_SUCCESS = new LongCounterMetric("image_write", MetricUnit.OPERATIONS,
                "counter of image succeed in write");
        COUNTER_IMAGE_WRITE_SUCCESS.addLabel(new MetricLabel("type", "success"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_IMAGE_WRITE_SUCCESS);
        COUNTER_IMAGE_WRITE_FAILED = new LongCounterMetric("image_write", MetricUnit.OPERATIONS,
                "counter of image failed to write");
        COUNTER_IMAGE_WRITE_FAILED.addLabel(new MetricLabel("type", "failed"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_IMAGE_WRITE_FAILED);

        COUNTER_IMAGE_PUSH_SUCCESS = new LongCounterMetric("image_push", MetricUnit.OPERATIONS,
                "counter of image succeeded in pushing to other frontends");
        COUNTER_IMAGE_PUSH_SUCCESS.addLabel(new MetricLabel("type", "success"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_IMAGE_PUSH_SUCCESS);
        COUNTER_IMAGE_PUSH_FAILED = new LongCounterMetric("image_push", MetricUnit.OPERATIONS,
                "counter of image failed to other frontends");
        COUNTER_IMAGE_PUSH_FAILED.addLabel(new MetricLabel("type", "failed"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_IMAGE_PUSH_FAILED);

        // image clean
        COUNTER_IMAGE_CLEAN_SUCCESS = new LongCounterMetric("image_clean", MetricUnit.OPERATIONS,
                "counter of image succeeded in cleaning");
        COUNTER_IMAGE_CLEAN_SUCCESS.addLabel(new MetricLabel("type", "success"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_IMAGE_CLEAN_SUCCESS);
        COUNTER_IMAGE_CLEAN_FAILED = new LongCounterMetric("image_clean", MetricUnit.OPERATIONS,
                "counter of image failed to clean");
        COUNTER_IMAGE_CLEAN_FAILED.addLabel(new MetricLabel("type", "failed"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_IMAGE_CLEAN_FAILED);

        // txn
        COUNTER_TXN_REJECT = new LongCounterMetric("txn_counter", MetricUnit.REQUESTS,
                "counter of rejected transactions");
        COUNTER_TXN_REJECT.addLabel(new MetricLabel("type", "reject"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_TXN_REJECT);
        COUNTER_TXN_BEGIN = new LongCounterMetric("txn_counter", MetricUnit.REQUESTS,
                "counter of beginning transactions");
        COUNTER_TXN_BEGIN.addLabel(new MetricLabel("type", "begin"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_TXN_BEGIN);
        COUNTER_TXN_SUCCESS = new LongCounterMetric("txn_counter", MetricUnit.REQUESTS,
                "counter of success transactions");
        COUNTER_TXN_SUCCESS.addLabel(new MetricLabel("type", "success"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_TXN_SUCCESS);
        COUNTER_TXN_FAILED = new LongCounterMetric("txn_counter", MetricUnit.REQUESTS,
                "counter of failed transactions");
        COUNTER_TXN_FAILED.addLabel(new MetricLabel("type", "failed"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_TXN_FAILED);
        COUNTER_UPDATE_TABLET_STAT_FAILED = new LongCounterMetric("update_tablet_stat_failed", MetricUnit.REQUESTS,
            "counter of failed to update tablet stat");
        COUNTER_UPDATE_TABLET_STAT_FAILED.addLabel(new MetricLabel("type", "failed"));
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_UPDATE_TABLET_STAT_FAILED);
        HISTO_TXN_EXEC_LATENCY = METRIC_REGISTER.histogram(
            MetricRegistry.name("txn", "exec", "latency", "ms"));
        HISTO_TXN_PUBLISH_LATENCY = METRIC_REGISTER.histogram(
            MetricRegistry.name("txn", "publish", "latency", "ms"));
        GaugeMetric<Long> txnNum = new GaugeMetric<Long>("txn_num", MetricUnit.NOUNIT,
                "number of running transactions") {
            @Override
            public Long getValue() {
                return Env.getCurrentGlobalTransactionMgr().getAllRunningTxnNum();
            }
        };
        DORIS_METRIC_REGISTER.addMetrics(txnNum);
        DB_GAUGE_TXN_NUM = addLabeledMetrics("db", () ->
                new GaugeMetricImpl<>("txn_num", MetricUnit.NOUNIT, "number of running transactions", 0L));
        GaugeMetric<Long> publishTxnNum = new GaugeMetric<Long>("publish_txn_num", MetricUnit.NOUNIT,
                "number of publish transactions") {
            @Override
            public Long getValue() {
                return Env.getCurrentGlobalTransactionMgr().getAllPublishTxnNum();
            }
        };
        DORIS_METRIC_REGISTER.addMetrics(publishTxnNum);
        DB_GAUGE_PUBLISH_TXN_NUM = addLabeledMetrics("db",
                () -> new GaugeMetricImpl<>("publish_txn_num", MetricUnit.NOUNIT,
                "number of publish transactions", 0L));
        COUNTER_ROUTINE_LOAD_ROWS = new LongCounterMetric("routine_load_rows", MetricUnit.ROWS,
                "total rows of routine load");
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_ROUTINE_LOAD_ROWS);
        COUNTER_ROUTINE_LOAD_RECEIVED_BYTES = new LongCounterMetric("routine_load_receive_bytes", MetricUnit.BYTES,
                "total received bytes of routine load");
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_ROUTINE_LOAD_RECEIVED_BYTES);
        COUNTER_ROUTINE_LOAD_ERROR_ROWS = new LongCounterMetric("routine_load_error_rows", MetricUnit.ROWS,
                "total error rows of routine load");
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_ROUTINE_LOAD_ERROR_ROWS);
        COUNTER_ROUTINE_LOAD_GET_META_LANTENCY = new LongCounterMetric("routine_load_get_meta_latency",
                MetricUnit.MILLISECONDS, "get meta lantency of routine load");
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_ROUTINE_LOAD_GET_META_LANTENCY);
        COUNTER_ROUTINE_LOAD_GET_META_COUNT = new LongCounterMetric("routine_load_get_meta_count", MetricUnit.NOUNIT,
                "get meta count of routine load");
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_ROUTINE_LOAD_GET_META_COUNT);
        COUNTER_ROUTINE_LOAD_GET_META_FAIL_COUNT = new LongCounterMetric("routine_load_get_meta_fail_count",
                MetricUnit.NOUNIT, "get meta fail count of routine load");
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_ROUTINE_LOAD_GET_META_FAIL_COUNT);
        COUNTER_ROUTINE_LOAD_TASK_EXECUTE_TIME = new LongCounterMetric("routine_load_task_execute_time",
                MetricUnit.MILLISECONDS, "task execute time of routine load");
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_ROUTINE_LOAD_TASK_EXECUTE_TIME);
        COUNTER_ROUTINE_LOAD_TASK_EXECUTE_COUNT = new LongCounterMetric("routine_load_task_execute_count",
                MetricUnit.MILLISECONDS, "task execute count of routine load");
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_ROUTINE_LOAD_TASK_EXECUTE_COUNT);

        COUNTER_HIT_SQL_BLOCK_RULE = new LongCounterMetric("counter_hit_sql_block_rule", MetricUnit.ROWS,
                "total hit sql block rule query");
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_HIT_SQL_BLOCK_RULE);

        THRIFT_COUNTER_RPC_ALL = addLabeledMetrics("method", () ->
                new LongCounterMetric("thrift_rpc_total", MetricUnit.NOUNIT, ""));
        THRIFT_COUNTER_RPC_LATENCY = addLabeledMetrics("method", () ->
                new LongCounterMetric("thrift_rpc_latency_ms", MetricUnit.MILLISECONDS, ""));

        // copy into
        HTTP_COUNTER_COPY_INFO_UPLOAD_REQUEST = new LongCounterMetric("http_copy_into_upload_request_total",
                MetricUnit.REQUESTS, "http copy into upload total request");
        DORIS_METRIC_REGISTER.addMetrics(HTTP_COUNTER_COPY_INFO_UPLOAD_REQUEST);
        HTTP_COUNTER_COPY_INFO_UPLOAD_ERR = new LongCounterMetric("http_copy_into_upload_err_total",
                MetricUnit.REQUESTS, "http copy into upload err request");
        DORIS_METRIC_REGISTER.addMetrics(HTTP_COUNTER_COPY_INFO_UPLOAD_ERR);
        HTTP_COUNTER_COPY_INFO_QUERY_REQUEST = new LongCounterMetric("http_copy_into_query_request_total",
                MetricUnit.REQUESTS, "http copy into total query request");
        DORIS_METRIC_REGISTER.addMetrics(HTTP_COUNTER_COPY_INFO_QUERY_REQUEST);
        HTTP_COUNTER_COPY_INFO_QUERY_ERR = new LongCounterMetric("http_copy_into_upload_err_total",
                MetricUnit.REQUESTS, "http copy into err query request");
        DORIS_METRIC_REGISTER.addMetrics(HTTP_COUNTER_COPY_INFO_QUERY_ERR);
        HISTO_HTTP_COPY_INTO_UPLOAD_LATENCY = METRIC_REGISTER.histogram(
            MetricRegistry.name("http_copy_into_upload", "latency", "ms"));
        HISTO_HTTP_COPY_INTO_QUERY_LATENCY = METRIC_REGISTER.histogram(
            MetricRegistry.name("http_copy_into_query", "latency", "ms"));

        HISTO_COMMIT_AND_PUBLISH_LATENCY = METRIC_REGISTER.histogram(
                MetricRegistry.name("txn_commit_and_publish", "latency", "ms"));

        GaugeMetric<Integer> commitQueueLength = new GaugeMetric<Integer>("commit_queue_length",
                MetricUnit.NOUNIT, "commit queue length") {
            @Override
            public Integer getValue() {
                return Env.getCurrentEnv().getGlobalTransactionMgr().getQueueLength();
            }
        };
        DORIS_METRIC_REGISTER.addMetrics(commitQueueLength);

        HISTO_GET_DELETE_BITMAP_UPDATE_LOCK_LATENCY = METRIC_REGISTER.histogram(
                MetricRegistry.name("get_delete_bitmap_update_lock", "latency", "ms"));
        HISTO_GET_COMMIT_LOCK_LATENCY = METRIC_REGISTER.histogram(
                MetricRegistry.name("get_commit_lock", "latency", "ms"));
        HISTO_CALCULATE_DELETE_BITMAP_LATENCY = METRIC_REGISTER.histogram(
                MetricRegistry.name("calculate_delete_bitmap", "latency", "ms"));
        HISTO_COMMIT_TO_MS_LATENCY = METRIC_REGISTER.histogram(
                MetricRegistry.name("commit_to_ms", "latency", "ms"));

        GAUGE_CATALOG_NUM = new GaugeMetric<Integer>("catalog_num",
                MetricUnit.NOUNIT, "total catalog num") {
            @Override
            public Integer getValue() {
                return Env.getCurrentEnv().getCatalogMgr().getCatalogNum();
            }
        };
        DORIS_METRIC_REGISTER.addMetrics(GAUGE_CATALOG_NUM);

        GAUGE_INTERNAL_DATABASE_NUM = new GaugeMetric<Integer>("internal_database_num",
                MetricUnit.NOUNIT, "total internal database num") {
            @Override
            public Integer getValue() {
                return Env.getCurrentEnv().getCatalogMgr().getInternalCatalog().getDbNum();
            }
        };
        DORIS_METRIC_REGISTER.addMetrics(GAUGE_INTERNAL_DATABASE_NUM);

        GAUGE_INTERNAL_TABLE_NUM = new GaugeMetric<Integer>("internal_table_num",
                MetricUnit.NOUNIT, "total internal table num") {
            @Override
            public Integer getValue() {
                return Env.getCurrentEnv().getCatalogMgr().getInternalCatalog().getAllDbs().stream()
                        .map(d -> (Database) d).map(Database::getTableNum).reduce(0, Integer::sum);
            }
        };
        DORIS_METRIC_REGISTER.addMetrics(GAUGE_INTERNAL_TABLE_NUM);

        GAUGE_MAX_TABLE_SIZE_BYTES = new GaugeMetricImpl<>("max_table_size_bytes", MetricUnit.BYTES, "", 0L);
        DORIS_METRIC_REGISTER.addMetrics(GAUGE_MAX_TABLE_SIZE_BYTES);

        GAUGE_MAX_PARTITION_SIZE_BYTES = new GaugeMetricImpl<>("max_partition_size_bytes", MetricUnit.BYTES, "", 0L);
        DORIS_METRIC_REGISTER.addMetrics(GAUGE_MAX_PARTITION_SIZE_BYTES);

        GAUGE_MAX_TABLET_SIZE_BYTES = new GaugeMetricImpl<>("max_tablet_size_bytes", MetricUnit.BYTES, "", 0L);
        DORIS_METRIC_REGISTER.addMetrics(GAUGE_MAX_TABLET_SIZE_BYTES);

        GAUGE_MIN_TABLE_SIZE_BYTES = new GaugeMetricImpl<>("min_table_size_bytes", MetricUnit.BYTES, "", 0L);
        DORIS_METRIC_REGISTER.addMetrics(GAUGE_MIN_TABLE_SIZE_BYTES);

        GAUGE_MIN_PARTITION_SIZE_BYTES = new GaugeMetricImpl<>("min_partition_size_bytes", MetricUnit.BYTES, "", 0L);
        DORIS_METRIC_REGISTER.addMetrics(GAUGE_MIN_PARTITION_SIZE_BYTES);

        GAUGE_MIN_TABLET_SIZE_BYTES = new GaugeMetricImpl<>("min_tablet_size_bytes", MetricUnit.BYTES, "", 0L);
        DORIS_METRIC_REGISTER.addMetrics(GAUGE_MIN_TABLET_SIZE_BYTES);

        GAUGE_AVG_TABLE_SIZE_BYTES = new GaugeMetricImpl<>("avg_table_size_bytes", MetricUnit.BYTES, "", 0L);
        DORIS_METRIC_REGISTER.addMetrics(GAUGE_AVG_TABLE_SIZE_BYTES);

        GAUGE_AVG_PARTITION_SIZE_BYTES = new GaugeMetricImpl<>("avg_partition_size_bytes", MetricUnit.BYTES, "", 0L);
        DORIS_METRIC_REGISTER.addMetrics(GAUGE_AVG_PARTITION_SIZE_BYTES);

        GAUGE_AVG_TABLET_SIZE_BYTES = new GaugeMetricImpl<>("avg_tablet_size_bytes", MetricUnit.BYTES, "", 0L);
        DORIS_METRIC_REGISTER.addMetrics(GAUGE_AVG_TABLET_SIZE_BYTES);

        COUNTER_AGENT_TASK_REQUEST_TOTAL = new LongCounterMetric("agent_task_request_total", MetricUnit.NOUNIT,
                "total agent batch task request send to BE");
        DORIS_METRIC_REGISTER.addMetrics(COUNTER_AGENT_TASK_REQUEST_TOTAL);
        COUNTER_AGENT_TASK_TOTAL = addLabeledMetrics("task", () ->
                new LongCounterMetric("agent_task_total", MetricUnit.NOUNIT, "total agent task"));
        COUNTER_AGENT_TASK_RESEND_TOTAL = addLabeledMetrics("task", () ->
                new LongCounterMetric("agent_task_resend_total", MetricUnit.NOUNIT, "total agent task resend"));

        // init system metrics
        initSystemMetrics();
        CloudMetrics.init();

        updateMetrics();
        isInit = true;

        if (Config.enable_metric_calculator) {
            metricTimer.scheduleAtFixedRate(metricCalculator, 0, 15 * 1000L, TimeUnit.MILLISECONDS);
        }
    }

    private static void initRoutineLoadJobMetrics() {
        //  routine load jobs
        RoutineLoadManager routineLoadManager = Env.getCurrentEnv().getRoutineLoadManager();
        for (RoutineLoadJob.JobState jobState : RoutineLoadJob.JobState.values()) {
            if (jobState == RoutineLoadJob.JobState.PAUSED) {
                addRoutineLoadJobStateGaugeMetric(routineLoadManager, jobState, "USER_PAUSED",
                        job -> job.getPauseReason() != null
                            && job.getPauseReason().getCode() == InternalErrorCode.MANUAL_PAUSE_ERR);
                addRoutineLoadJobStateGaugeMetric(routineLoadManager, jobState, "ABNORMAL_PAUSED",
                        job -> job.getPauseReason() != null
                            && job.getPauseReason().getCode() != InternalErrorCode.MANUAL_PAUSE_ERR);
            }
            addRoutineLoadJobStateGaugeMetric(routineLoadManager, jobState, jobState.name(), job -> true);
        }
        GAUGE_ROUTINE_LOAD_PROGRESS = new GaugeMetric<Long>("routine_load_progress",
                MetricUnit.NOUNIT, "total routine load progress") {
            @Override
            public Long getValue() {
                if (!Env.getCurrentEnv().isMaster()) {
                    return 0L;
                }
                return routineLoadManager
                        .getActiveRoutineLoadJobs().stream()
                        .mapToLong(RoutineLoadJob::totalProgress)
                        .sum();
            }
        };
        DORIS_METRIC_REGISTER.addMetrics(GAUGE_ROUTINE_LOAD_PROGRESS);
        GAUGE_ROUTINE_LOAD_LAG = new GaugeMetric<Long>("routine_load_lag",
                MetricUnit.NOUNIT, "total routine load lag") {
            @Override
            public Long getValue() {
                if (!Env.getCurrentEnv().isMaster()) {
                    return 0L;
                }
                return routineLoadManager
                        .getActiveRoutineLoadJobs().stream()
                        .mapToLong(RoutineLoadJob::totalLag)
                        .sum();
            }
        };
        DORIS_METRIC_REGISTER.addMetrics(GAUGE_ROUTINE_LOAD_LAG);
        GAUGE_ROUTINE_LOAD_ABORT_TASK_NUM = new GaugeMetric<Long>("routine_load_abort_task_num",
                MetricUnit.NOUNIT, "total number of aborted tasks in active routine load jobs") {
            @Override
            public Long getValue() {
                if (!Env.getCurrentEnv().isMaster()) {
                    return 0L;
                }
                return routineLoadManager
                        .getActiveRoutineLoadJobs().stream()
                        .mapToLong(job -> job.getRoutineLoadStatistic().abortedTaskNum)
                        .sum();
            }
        };
        DORIS_METRIC_REGISTER.addMetrics(GAUGE_ROUTINE_LOAD_ABORT_TASK_NUM);
    }

    private static void addRoutineLoadJobStateGaugeMetric(RoutineLoadManager routineLoadManager,
                                            RoutineLoadJob.JobState jobState,
                                            String stateLabel, Predicate<RoutineLoadJob> filter) {
        GaugeMetric<Long> gauge = new GaugeMetric<Long>("job", MetricUnit.NOUNIT, "routine load job statistics") {
            @Override
            public Long getValue() {
                if (!Env.getCurrentEnv().isMaster()) {
                    return 0L;
                }
                return routineLoadManager
                        .getRoutineLoadJobByState(Collections.singleton(jobState))
                        .stream()
                        .filter(filter)
                        .count();
            }
        };
        gauge.addLabel(new MetricLabel("job", "load"))
                .addLabel(new MetricLabel("type", "ROUTINE_LOAD"))
                .addLabel(new MetricLabel("state", stateLabel));
        DORIS_METRIC_REGISTER.addMetrics(gauge);
    }

    private static void initSystemMetrics() {
        // TCP retransSegs
        GaugeMetric<Long> tcpRetransSegs = (GaugeMetric<Long>) new GaugeMetric<Long>(
                "snmp", MetricUnit.NOUNIT, "All TCP packets retransmitted") {
            @Override
            public Long getValue() {
                return SYSTEM_METRICS.tcpRetransSegs;
            }
        };
        tcpRetransSegs.addLabel(new MetricLabel("name", "tcp_retrans_segs"));
        DORIS_METRIC_REGISTER.addSystemMetrics(tcpRetransSegs);

        // TCP inErrs
        GaugeMetric<Long> tpcInErrs = (GaugeMetric<Long>) new GaugeMetric<Long>(
                "snmp", MetricUnit.NOUNIT, "The number of all problematic TCP packets received") {
            @Override
            public Long getValue() {
                return SYSTEM_METRICS.tcpInErrs;
            }
        };
        tpcInErrs.addLabel(new MetricLabel("name", "tcp_in_errs"));
        DORIS_METRIC_REGISTER.addSystemMetrics(tpcInErrs);

        // TCP inSegs
        GaugeMetric<Long> tpcInSegs = (GaugeMetric<Long>) new GaugeMetric<Long>(
                "snmp", MetricUnit.NOUNIT, "The number of all TCP packets received") {
            @Override
            public Long getValue() {
                return SYSTEM_METRICS.tcpInSegs;
            }
        };
        tpcInSegs.addLabel(new MetricLabel("name", "tcp_in_segs"));
        DORIS_METRIC_REGISTER.addSystemMetrics(tpcInSegs);

        // TCP outSegs
        GaugeMetric<Long> tpcOutSegs = (GaugeMetric<Long>) new GaugeMetric<Long>(
                "snmp", MetricUnit.NOUNIT, "The number of all TCP packets send with RST") {
            @Override
            public Long getValue() {
                return SYSTEM_METRICS.tcpOutSegs;
            }
        };
        tpcOutSegs.addLabel(new MetricLabel("name", "tcp_out_segs"));
        DORIS_METRIC_REGISTER.addSystemMetrics(tpcOutSegs);

        // Memory Total
        GaugeMetric<Long> memTotal = (GaugeMetric<Long>) new GaugeMetric<Long>(
                "meminfo", MetricUnit.BYTES, "Total usable memory") {
            @Override
            public Long getValue() {
                return SYSTEM_METRICS.memTotal;
            }
        };
        memTotal.addLabel(new MetricLabel("name", "memory_total"));
        DORIS_METRIC_REGISTER.addSystemMetrics(memTotal);

        // Memory Free
        GaugeMetric<Long> memFree = (GaugeMetric<Long>) new GaugeMetric<Long>(
                "meminfo", MetricUnit.BYTES, "The amount of physical memory not used by the system") {
            @Override
            public Long getValue() {
                return SYSTEM_METRICS.memFree;
            }
        };
        memFree.addLabel(new MetricLabel("name", "memory_free"));
        DORIS_METRIC_REGISTER.addSystemMetrics(memFree);

        // Memory Total
        GaugeMetric<Long> memAvailable = (GaugeMetric<Long>) new GaugeMetric<Long>("meminfo", MetricUnit.BYTES,
                "An estimate of how much memory is available for starting new applications, without swapping") {
            @Override
            public Long getValue() {
                return SYSTEM_METRICS.memAvailable;
            }
        };
        memAvailable.addLabel(new MetricLabel("name", "memory_available"));
        DORIS_METRIC_REGISTER.addSystemMetrics(memAvailable);

        // Buffers
        GaugeMetric<Long> buffers = (GaugeMetric<Long>) new GaugeMetric<Long>("meminfo", MetricUnit.BYTES,
                "Memory in buffer cache, so relatively temporary storage for raw disk blocks") {
            @Override
            public Long getValue() {
                return SYSTEM_METRICS.buffers;
            }
        };
        buffers.addLabel(new MetricLabel("name", "buffers"));
        DORIS_METRIC_REGISTER.addSystemMetrics(buffers);

        // Cached
        GaugeMetric<Long> cached = (GaugeMetric<Long>) new GaugeMetric<Long>(
                "meminfo", MetricUnit.BYTES, "Memory in the pagecache (Diskcache and Shared Memory)") {
            @Override
            public Long getValue() {
                return SYSTEM_METRICS.cached;
            }
        };
        cached.addLabel(new MetricLabel("name", "cached"));
        DORIS_METRIC_REGISTER.addSystemMetrics(cached);
    }

    // to generate the metrics related to tablets of each backends
    // this metric is reentrant, so that we can add or remove metric along with the backend add or remove
    // at runtime.
    public static void generateBackendsTabletMetrics() {
        // remove all previous 'tablet' metric
        DORIS_METRIC_REGISTER.removeMetrics(TABLET_NUM);
        DORIS_METRIC_REGISTER.removeMetrics(TABLET_MAX_COMPACTION_SCORE);

        SystemInfoService infoService = Env.getCurrentSystemInfo();

        for (Long beId : infoService.getAllBackendIds(false)) {
            Backend be = infoService.getBackend(beId);
            if (be == null) {
                continue;
            }

            // tablet number of each backends
            GaugeMetric<Long> tabletNum = new GaugeMetric<Long>(TABLET_NUM, MetricUnit.NOUNIT, "tablet number") {
                @Override
                public Long getValue() {
                    if (!Env.getCurrentEnv().isMaster()) {
                        return 0L;
                    }
                    return (long) infoService.getTabletNumByBackendId(beId);
                }
            };
            tabletNum.addLabel(new MetricLabel("backend",
                    NetUtils.getHostPortInAccessibleFormat(be.getHost(), be.getHeartbeatPort())));
            DORIS_METRIC_REGISTER.addMetrics(tabletNum);

            // max compaction score of tablets on each backends
            GaugeMetric<Long> tabletMaxCompactionScore = new GaugeMetric<Long>(TABLET_MAX_COMPACTION_SCORE,
                    MetricUnit.NOUNIT, "tablet max compaction score") {
                @Override
                public Long getValue() {
                    if (!Env.getCurrentEnv().isMaster()) {
                        return 0L;
                    }
                    return be.getTabletMaxCompactionScore();
                }
            };
            tabletMaxCompactionScore.addLabel(new MetricLabel("backend",
                    NetUtils.getHostPortInAccessibleFormat(be.getHost(), be.getHeartbeatPort())));
            DORIS_METRIC_REGISTER.addMetrics(tabletMaxCompactionScore);

        } // end for backends
    }

    public static synchronized String getMetric(MetricVisitor visitor) {
        if (!isInit) {
            return "";
        }

        // update the metrics first
        updateMetrics();

        // update load job metrics
        updateLoadJobMetrics();

        // jvm
        JvmService jvmService = new JvmService();
        JvmStats jvmStats = jvmService.stats();
        visitor.visitJvm(jvmStats);

        // doris metrics and system metrics.
        DORIS_METRIC_REGISTER.accept(visitor);

        // histogram
        SortedMap<String, Histogram> histograms = METRIC_REGISTER.getHistograms();
        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            visitor.visitHistogram(MetricVisitor.FE_PREFIX, entry.getKey(), entry.getValue());
        }

        visitor.visitNodeInfo();

        visitor.visitCloudTableStats();

        visitor.visitWorkloadGroup();

        return visitor.finish();
    }

    public static <M extends Metric<?>> AutoMappedMetric<M> addLabeledMetrics(String label, Supplier<M> metric) {
        return new AutoMappedMetric<>(value -> {
            M m = metric.get();
            m.addLabel(new MetricLabel(label, value));
            MetricRepo.DORIS_METRIC_REGISTER.addMetrics(m);
            return m;
        });
    }

    // update some metrics to make a ready to be visited
    private static void updateMetrics() {
        SYSTEM_METRICS.update();
    }

    public static synchronized List<Metric> getMetricsByName(String name) {
        return DORIS_METRIC_REGISTER.getMetricsByName(name);
    }

    private static void updateLoadJobMetrics() {
        LoadManager loadManager = Env.getCurrentEnv().getLoadManager();
        MetricRepo.loadJobNum = loadManager.getLoadJobNum();
    }

    private static long getLoadJobNum(EtlJobType jobType, JobState jobState) {
        return MetricRepo.loadJobNum.getOrDefault(Pair.of(jobType, jobState), 0L);
    }

    public static void registerCloudMetrics(String clusterId, String clusterName) {
        if (!MetricRepo.isInit || Config.isNotCloudMode() || Strings.isNullOrEmpty(clusterName)
                || Strings.isNullOrEmpty(clusterId)) {
            return;
        }
        List<MetricLabel> labels = new ArrayList<>();
        labels.add(new MetricLabel("cluster_id", clusterId));
        labels.add(new MetricLabel("cluster_name", clusterName));

        LongCounterMetric requestAllCounter = CloudMetrics.CLUSTER_REQUEST_ALL_COUNTER.getOrAdd(clusterId);
        requestAllCounter.setLabels(labels);
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(requestAllCounter);

        LongCounterMetric queryAllCounter = CloudMetrics.CLUSTER_QUERY_ALL_COUNTER.getOrAdd(clusterId);
        queryAllCounter.setLabels(labels);
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(queryAllCounter);

        LongCounterMetric queryErrCounter = CloudMetrics.CLUSTER_QUERY_ERR_COUNTER.getOrAdd(clusterId);
        queryErrCounter.setLabels(labels);
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(queryErrCounter);

        GaugeMetricImpl<Double> requestPerSecondGauge = CloudMetrics.CLUSTER_REQUEST_PER_SECOND_GAUGE
                .getOrAdd(clusterId);
        requestPerSecondGauge.setLabels(labels);
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(requestPerSecondGauge);

        GaugeMetricImpl<Double> queryPerSecondGauge = CloudMetrics.CLUSTER_QUERY_PER_SECOND_GAUGE.getOrAdd(clusterId);
        queryPerSecondGauge.setLabels(labels);
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(queryPerSecondGauge);

        GaugeMetricImpl<Double> queryErrRateGauge = CloudMetrics.CLUSTER_QUERY_ERR_RATE_GAUGE.getOrAdd(clusterId);
        queryErrRateGauge.setLabels(labels);
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(queryErrRateGauge);

        String key = clusterId + CloudMetrics.CLOUD_CLUSTER_DELIMITER + clusterName;
        CloudMetrics.CLUSTER_QUERY_LATENCY_HISTO.getOrAdd(key);
    }

    public static void increaseClusterRequestAll(String clusterName) {
        if (!MetricRepo.isInit || Config.isNotCloudMode() || Strings.isNullOrEmpty(clusterName)) {
            return;
        }
        String clusterId = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                .getCloudClusterNameToId().get(clusterName);
        if (Strings.isNullOrEmpty(clusterId)) {
            return;
        }
        LongCounterMetric counter = CloudMetrics.CLUSTER_REQUEST_ALL_COUNTER.getOrAdd(clusterId);
        List<MetricLabel> labels = new ArrayList<>();
        counter.increase(1L);
        labels.add(new MetricLabel("cluster_id", clusterId));
        labels.add(new MetricLabel("cluster_name", clusterName));
        counter.setLabels(labels);
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(counter);
    }

    public static void increaseClusterQueryAll(String clusterName) {
        if (!MetricRepo.isInit || Config.isNotCloudMode() || Strings.isNullOrEmpty(clusterName)) {
            return;
        }
        String clusterId = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                .getCloudClusterNameToId().get(clusterName);
        if (Strings.isNullOrEmpty(clusterId)) {
            return;
        }
        LongCounterMetric counter = CloudMetrics.CLUSTER_QUERY_ALL_COUNTER.getOrAdd(clusterId);
        List<MetricLabel> labels = new ArrayList<>();
        counter.increase(1L);
        labels.add(new MetricLabel("cluster_id", clusterId));
        labels.add(new MetricLabel("cluster_name", clusterName));
        counter.setLabels(labels);
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(counter);
    }

    public static void increaseClusterQueryErr(String clusterName) {
        if (!MetricRepo.isInit || Config.isNotCloudMode() || Strings.isNullOrEmpty(clusterName)) {
            return;
        }
        String clusterId = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                .getCloudClusterNameToId().get(clusterName);
        if (Strings.isNullOrEmpty(clusterId)) {
            return;
        }
        LongCounterMetric counter = CloudMetrics.CLUSTER_QUERY_ERR_COUNTER.getOrAdd(clusterId);
        List<MetricLabel> labels = new ArrayList<>();
        counter.increase(1L);
        labels.add(new MetricLabel("cluster_id", clusterId));
        labels.add(new MetricLabel("cluster_name", clusterName));
        counter.setLabels(labels);
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(counter);
    }

    public static void updateClusterRequestPerSecond(String clusterId, double value, List<MetricLabel> labels) {
        if (!MetricRepo.isInit || Config.isNotCloudMode() || Strings.isNullOrEmpty(clusterId)) {
            return;
        }
        GaugeMetricImpl<Double> gauge = CloudMetrics.CLUSTER_REQUEST_PER_SECOND_GAUGE.getOrAdd(clusterId);
        gauge.setValue(value);
        gauge.setLabels(labels);
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(gauge);
    }

    public static void updateClusterQueryPerSecond(String clusterId, double value, List<MetricLabel> labels) {
        if (!MetricRepo.isInit || Config.isNotCloudMode() || Strings.isNullOrEmpty(clusterId)) {
            return;
        }
        GaugeMetricImpl<Double> gauge = CloudMetrics.CLUSTER_QUERY_PER_SECOND_GAUGE.getOrAdd(clusterId);
        gauge.setValue(value);
        gauge.setLabels(labels);
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(gauge);
    }

    public static void updateClusterQueryErrRate(String clusterId, double value, List<MetricLabel> labels) {
        if (!MetricRepo.isInit || Config.isNotCloudMode() || Strings.isNullOrEmpty(clusterId)) {
            return;
        }
        GaugeMetricImpl<Double> gauge = CloudMetrics.CLUSTER_QUERY_ERR_RATE_GAUGE.getOrAdd(clusterId);
        gauge.setValue(value);
        gauge.setLabels(labels);
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(gauge);
    }

    public static void updateClusterBackendAlive(String clusterName, String clusterId, String ipAddress,
            boolean alive) {
        if (!MetricRepo.isInit || Config.isNotCloudMode() || Strings.isNullOrEmpty(clusterName)
                || Strings.isNullOrEmpty(clusterId) || Strings.isNullOrEmpty(ipAddress)) {
            return;
        }
        String key = clusterId + "_" + ipAddress;
        GaugeMetricImpl<Integer> metric = CloudMetrics.CLUSTER_BACKEND_ALIVE.getOrAdd(key);
        metric.setValue(alive ? 1 : 0);
        List<MetricLabel> labels = new ArrayList<>();
        labels.add(new MetricLabel("cluster_id", clusterId));
        labels.add(new MetricLabel("cluster_name", clusterName));
        labels.add(new MetricLabel("address", ipAddress));
        metric.setLabels(labels);
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(metric);
    }

    public static void updateClusterBackendAliveTotal(String clusterName, String clusterId, int aliveNum) {
        if (!MetricRepo.isInit || Config.isNotCloudMode() || Strings.isNullOrEmpty(clusterName)
                || Strings.isNullOrEmpty(clusterId)) {
            return;
        }
        GaugeMetricImpl<Integer> gauge = CloudMetrics.CLUSTER_BACKEND_ALIVE_TOTAL.getOrAdd(clusterId);
        gauge.setValue(aliveNum);
        List<MetricLabel> labels = new ArrayList<>();
        labels.add(new MetricLabel("cluster_id", clusterId));
        labels.add(new MetricLabel("cluster_name", clusterName));
        gauge.setLabels(labels);
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(gauge);
    }

    public static void updateClusterQueryLatency(String clusterName, long elapseMs) {
        if (!MetricRepo.isInit || Config.isNotCloudMode() || Strings.isNullOrEmpty(clusterName)) {
            return;
        }
        String clusterId = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                .getCloudClusterNameToId().get(clusterName);
        if (Strings.isNullOrEmpty(clusterId)) {
            return;
        }
        String key = clusterId + CloudMetrics.CLOUD_CLUSTER_DELIMITER + clusterName;
        CloudMetrics.CLUSTER_QUERY_LATENCY_HISTO.getOrAdd(key).update(elapseMs);
    }
}
