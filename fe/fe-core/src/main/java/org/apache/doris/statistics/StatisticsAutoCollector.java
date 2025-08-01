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

package org.apache.doris.statistics;

import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.persist.TableStatsDeletionLog;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.AnalysisInfo.ScheduleType;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class StatisticsAutoCollector extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(StatisticsAutoCollector.class);

    protected final AnalysisTaskExecutor analysisTaskExecutor;
    // Waited flag. Wait once when FE started for TabletStatMgr has received BE report at least once.
    // This couldn't guarantee getRowCount will return up-to-date value,
    // but could reduce the chance to get wrong row count. e.g. 0 after FE restart.
    private boolean waited = false;

    public StatisticsAutoCollector() {
        super("Automatic Analyzer", TimeUnit.MINUTES.toMillis(Config.auto_check_statistics_in_minutes));
        this.analysisTaskExecutor = new AnalysisTaskExecutor(Config.auto_analyze_simultaneously_running_task_num,
                StatisticConstants.TASK_QUEUE_CAP, "Auto Analysis Job Executor");
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!Env.getCurrentEnv().isMaster()) {
            return;
        }
        if (!StatisticsUtil.statsTblAvailable()) {
            LOG.info("Stats table not available, skip");
            return;
        }
        if (Env.isCheckpointThread()) {
            return;
        }
        if (waited) {
            collect();
        } else {
            try {
                Thread.sleep((long) Config.tablet_stat_update_interval_second * 1000 * 2);
                waited = true;
            } catch (InterruptedException e) {
                LOG.info("Wait Sleep interrupted.", e);
            }
        }
    }

    protected void collect() {
        while (StatisticsUtil.canCollect()) {
            Pair<Entry<TableName, Set<Pair<String, String>>>, JobPriority> job = getJob();
            if (job == null) {
                // No more job to process, break and sleep.
                LOG.info("No auto analyze jobs to process.");
                break;
            }
            try {
                TableName tblName = job.first.getKey();
                TableIf table = StatisticsUtil.findTable(tblName.getCtl(), tblName.getDb(), tblName.getTbl());
                if (!supportAutoAnalyze(table)) {
                    continue;
                }
                processOneJob(table, job.first.getValue(), job.second);
            } catch (Exception e) {
                LOG.warn("Failed to analyze table {} with columns [{}]", job.first.getKey().getTbl(),
                        job.first.getValue().stream().map(Pair::toString).collect(Collectors.joining(",")), e);
            }
        }
    }

    protected Pair<Entry<TableName, Set<Pair<String, String>>>, JobPriority> getJob() {
        AnalysisManager manager = Env.getServingEnv().getAnalysisManager();
        Optional<Entry<TableName, Set<Pair<String, String>>>> job = fetchJobFromMap(manager.highPriorityJobs);
        if (job.isPresent()) {
            return Pair.of(job.get(), JobPriority.HIGH);
        }
        job = fetchJobFromMap(manager.midPriorityJobs);
        if (job.isPresent()) {
            return Pair.of(job.get(), JobPriority.MID);
        }
        job = fetchJobFromMap(manager.lowPriorityJobs);
        if (job.isPresent()) {
            return Pair.of(job.get(), JobPriority.LOW);
        }
        job = fetchJobFromMap(manager.veryLowPriorityJobs);
        return job.map(tableNameSetEntry -> Pair.of(tableNameSetEntry, JobPriority.VERY_LOW)).orElse(null);
    }

    protected Optional<Map.Entry<TableName, Set<Pair<String, String>>>> fetchJobFromMap(
            Map<TableName, Set<Pair<String, String>>> jobMap) {
        synchronized (jobMap) {
            Optional<Map.Entry<TableName, Set<Pair<String, String>>>> first = jobMap.entrySet().stream().findFirst();
            first.ifPresent(entry -> jobMap.remove(entry.getKey()));
            return first;
        }
    }

    protected void processOneJob(TableIf table, Set<Pair<String, String>> columns, JobPriority priority) {
        AnalysisMethod analysisMethod = (StatisticsUtil.getHugeTableLowerBoundSizeInBytes() == 0
                || table.getDataSize(true) >= StatisticsUtil.getHugeTableLowerBoundSizeInBytes())
                ? AnalysisMethod.SAMPLE : AnalysisMethod.FULL;
        if (StatisticsUtil.enablePartitionAnalyze() && table.isPartitionedTable()) {
            analysisMethod = AnalysisMethod.FULL;
        }
        boolean isSampleAnalyze = analysisMethod.equals(AnalysisMethod.SAMPLE);
        OlapTable olapTable = table instanceof OlapTable ? (OlapTable) table : null;
        AnalysisManager manager = Env.getServingEnv().getAnalysisManager();
        TableStatsMeta tableStatsStatus = manager.findTableStatsStatus(table.getId());
        long rowCount = table.getRowCount();
        if (!readyToSample(table, rowCount, manager, tableStatsStatus, isSampleAnalyze)) {
            return;
        }
        appendAllColumns(table, columns);
        long olapTableVersion = StatisticsUtil.getOlapTableVersion(olapTable);
        columns = columns.stream()
                .filter(c -> StatisticsUtil.needAnalyzeColumn(table, c)
                        || StatisticsUtil.isLongTimeColumn(table, c, olapTableVersion))
                .filter(c -> olapTable == null || StatisticsUtil.canCollectColumn(
                        olapTable.getIndexMetaByIndexId(olapTable.getIndexIdByName(c.first)).getColumnByName(c.second),
                        table, isSampleAnalyze, olapTable.getIndexIdByName(c.first)))
            .collect(Collectors.toSet());
        AnalysisInfo analyzeJob = createAnalyzeJobForTbl(table, columns, priority, analysisMethod,
                rowCount, tableStatsStatus, olapTableVersion);
        if (analyzeJob == null) {
            return;
        }
        LOG.debug("Auto analyze job : {}", analyzeJob.toString());
        try {
            executeSystemAnalysisJob(analyzeJob);
        } catch (Exception e) {
            StringJoiner stringJoiner = new StringJoiner(",", "[", "]");
            for (Pair<String, String> pair : columns) {
                stringJoiner.add(pair.toString());
            }
            LOG.warn("Fail to auto analyze table {}, columns [{}]", table.getName(), stringJoiner.toString());
        }
    }

    protected boolean readyToSample(TableIf table, long rowCount, AnalysisManager manager,
            TableStatsMeta tableStatsStatus, boolean isSample) {
        if (!isSample) {
            return true;
        }
        OlapTable olapTable = table instanceof OlapTable ? (OlapTable) table : null;
        if (olapTable != null
                && olapTable.getRowCountForIndex(olapTable.getBaseIndexId(), true) == TableIf.UNKNOWN_ROW_COUNT) {
            LOG.info("Table {} row count is not fully reported, skip auto analyzing it.", olapTable.getName());
            return false;
        }
        // We don't auto analyze empty table to avoid all 0 stats.
        // Because all 0 is more dangerous than unknown stats when row count report is delayed.
        if (rowCount <= 0) {
            LOG.info("Table {} is empty, remove its old stats and skip auto analyze it.", table.getName());
            // Remove the table's old stats if exists.
            if (tableStatsStatus != null && !tableStatsStatus.isColumnsStatsEmpty()) {
                manager.removeTableStats(table.getId());
                Env.getCurrentEnv().getEditLog().logDeleteTableStats(new TableStatsDeletionLog(table.getId()));
                manager.dropStats(table, null);
            }
            return false;
        }
        return true;
    }

    // If partition changed (partition first loaded, partition dropped and so on), need re-analyze all columns.
    private void appendAllColumns(TableIf table, Set<Pair<String, String>> columns) {
        if (!(table instanceof OlapTable)) {
            return;
        }
        AnalysisManager manager = Env.getServingEnv().getAnalysisManager();
        TableStatsMeta tableStatsStatus = manager.findTableStatsStatus(table.getId());
        if (tableStatsStatus != null && tableStatsStatus.partitionChanged.get()) {
            OlapTable olapTable = (OlapTable) table;
            Set<String> allColumnPairs = olapTable.getSchemaAllIndexes(false).stream()
                    .filter(c -> !StatisticsUtil.isUnsupportedType(c.getType()))
                    .map(Column::getName)
                    .collect(Collectors.toSet());
            columns.addAll(olapTable.getColumnIndexPairs(allColumnPairs));
        }
    }

    protected boolean supportAutoAnalyze(TableIf tableIf) {
        if (tableIf == null) {
            return false;
        }
        return tableIf instanceof OlapTable
                || tableIf instanceof HMSExternalTable
                && ((HMSExternalTable) tableIf).getDlaType().equals(HMSExternalTable.DLAType.HIVE);
    }

    protected AnalysisInfo createAnalyzeJobForTbl(TableIf table, Set<Pair<String, String>> jobColumns,
            JobPriority priority, AnalysisMethod analysisMethod, long rowCount, TableStatsMeta tableStatsStatus,
            long version) {
        if (jobColumns == null || jobColumns.isEmpty()) {
            return null;
        }
        LOG.info("Auto analyze table {} row count is {}", table.getName(), rowCount);
        StringJoiner stringJoiner = new StringJoiner(",", "[", "]");
        for (Pair<String, String> pair : jobColumns) {
            stringJoiner.add(pair.toString());
        }
        return new AnalysisInfoBuilder()
                .setJobId(Env.getCurrentEnv().getNextId())
                .setCatalogId(table.getDatabase().getCatalog().getId())
                .setDBId(table.getDatabase().getId())
                .setTblId(table.getId())
                .setColName(stringJoiner.toString())
                .setJobColumns(jobColumns)
                .setAnalysisType(AnalysisInfo.AnalysisType.FUNDAMENTALS)
                .setAnalysisMethod(analysisMethod)
                .setPartitionNames(Collections.emptySet())
                .setSampleRows(analysisMethod.equals(AnalysisMethod.SAMPLE)
                    ? StatisticsUtil.getHugeTableSampleRows() : -1)
                .setScheduleType(ScheduleType.AUTOMATIC)
                .setState(AnalysisState.PENDING)
                .setTaskIds(new ArrayList<>())
                .setLastExecTimeInMs(System.currentTimeMillis())
                .setJobType(JobType.SYSTEM)
                .setTblUpdateTime(table.getUpdateTime())
                .setRowCount(rowCount)
                .setUpdateRows(tableStatsStatus == null ? 0 : tableStatsStatus.updatedRows.get())
                .setTableVersion(version)
                .setPriority(priority)
                .setPartitionUpdateRows(tableStatsStatus == null ? null : tableStatsStatus.partitionUpdateRows)
                .setEnablePartition(StatisticsUtil.enablePartitionAnalyze())
                .build();
    }

    // Analysis job created by the system
    @VisibleForTesting
    protected void executeSystemAnalysisJob(AnalysisInfo jobInfo)
            throws DdlException, ExecutionException, InterruptedException {
        Map<Long, BaseAnalysisTask> analysisTasks = new HashMap<>();
        AnalysisManager analysisManager = Env.getCurrentEnv().getAnalysisManager();
        analysisManager.createTaskForEachColumns(jobInfo, analysisTasks, false);
        Env.getCurrentEnv().getAnalysisManager().constructJob(jobInfo, analysisTasks.values());
        Env.getCurrentEnv().getAnalysisManager().registerSysJob(jobInfo, analysisTasks);
        Future<?>[] futures = new Future[analysisTasks.values().size()];
        int i = 0;
        for (BaseAnalysisTask task : analysisTasks.values()) {
            futures[i++] = analysisTaskExecutor.submitTask(task);
        }
        for (Future future : futures) {
            future.get();
        }
    }

    public boolean isReady() {
        return waited;
    }
}
