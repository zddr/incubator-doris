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

package org.apache.doris.regression.suite

import com.google.common.collect.Maps
import com.google.gson.Gson
import org.apache.doris.regression.Config
import org.apache.doris.regression.json.PartitionData
import org.apache.doris.regression.json.PartitionRecords
import org.apache.doris.regression.suite.client.BackendClientImpl
import org.apache.doris.regression.suite.client.FrontendClientImpl
import org.apache.doris.regression.util.SyncerUtils
import org.apache.doris.regression.util.S3RepoValidate
import org.apache.doris.thrift.TBeginTxnResult
import org.apache.doris.thrift.TBinlog
import org.apache.doris.regression.json.BinlogData
import org.apache.doris.thrift.TBinlogType
import org.apache.doris.thrift.TCommitTxnResult
import org.apache.doris.thrift.TGetBinlogResult
import org.apache.doris.thrift.TGetMasterTokenResult
import org.apache.doris.thrift.TGetSnapshotResult
import org.apache.doris.thrift.TIngestBinlogRequest
import org.apache.doris.thrift.TIngestBinlogResult
import org.apache.doris.thrift.TNetworkAddress
import org.apache.doris.thrift.TRestoreSnapshotResult
import org.apache.doris.thrift.TStatus
import org.apache.doris.thrift.TStatusCode
import org.apache.doris.thrift.TSubTxnInfo
import org.apache.doris.thrift.TTabletCommitInfo
import org.apache.doris.thrift.TUniqueId
import org.apache.thrift.transport.TTransportException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import groovy.util.logging.Slf4j

import java.util.Map.Entry

@Slf4j
class Syncer {
    final SyncerContext context
    final Suite suite
    final Logger logger = LoggerFactory.getLogger(Syncer.class)

    Syncer(Suite suite, Config config) {
        this.suite = suite
        context = new SyncerContext(suite, suite.context.dbName, config)
    }

    enum ccrCluster {
        SOURCE,
        TARGET
    }

    Boolean checkEnableFeatureBinlog() {
        List<List<Object>> rows = suite.sql("SHOW FRONTEND CONFIG LIKE \"%%enable_feature_binlog%%\"")
        if (rows.size() >= 1 && (rows[0][0] as String).contains("enable_feature_binlog")) {
            return (rows[0][1] as String) == "true"
        }
        return false
    }

    private Boolean checkBinlog(TBinlog binlog, String table, Boolean update) {
        // step 1: check binlog availability
        if (binlog == null) {
            return false
        }

        // step 2: check and set seq to context
        if (binlog.isSetCommitSeq()) {
            context.seq = binlog.getCommitSeq()
            logger.info("Now last seq is ${context.seq}")
        } else {
            logger.error("Invalid binlog! binlog seq is unset.")
            return false
        }

        // step 3: print binlog type
        if (binlog.isSetType()) {
            String typeName
            switch (binlog.getType()) {
                case TBinlogType.UPSERT:
                    typeName = "UPSERT"
                    break
                case TBinlogType.CREATE_TABLE:
                    typeName = "CREATE_TABLE"
                    break
                case TBinlogType.ADD_PARTITION:
                    typeName = "ADD_PARTITION"
                    break
                default:
                    typeName = "UNKNOWN"
                    break
            }
            logger.info("binlog type name is ${typeName}")
        }

        // step 4: check binlog data is set and get metadata
        if (binlog.isSetData()) {
            String data = binlog.getData()
            logger.info("binlog data is ${data}")
            if (update) {
                Gson gson = new Gson()
                context.lastBinlog = gson.fromJson(data, BinlogData.class)
                logger.info("Source lastBinlog: ${context.lastBinlog}")
                if (context.lastBinlog.stids != null) {
                    context.sourceSubTxnIds = context.lastBinlog.stids as List
                }
                logger.info("source subTxnIds: ${context.sourceSubTxnIds}")
                context.txnInsert = context.sourceSubTxnIds.size() > 0
                context.targetSubTxnIds.clear()
                context.sourceToTargetSubTxnId.clear()
                context.subTxnInfos.clear()
                return getSourceMeta(table)
            }
        } else {
            logger.error("binlog data is not contains data!")
            return false
        }

        return true
    }

    private Boolean checkGetBinlog(String table, TGetBinlogResult result, Boolean update) {
        TBinlog binlog = null

        // step 1: check status
        if (result != null && result.isSetStatus()) {
            TStatus status = result.getStatus()
            if (status.isSetStatusCode()) {
                TStatusCode code = status.getStatusCode()
                switch (code) {
                    case TStatusCode.BINLOG_TOO_OLD_COMMIT_SEQ:
                    case TStatusCode.OK:
                        if (result.isSetBinlogs()) {
                            binlog = result.getBinlogs().first()
                        }
                        break
                    case TStatusCode.BINLOG_DISABLE:
                        logger.error("Binlog is disabled!")
                        break
                    case TStatusCode.BINLOG_TOO_NEW_COMMIT_SEQ:
                        logger.error("Binlog is too new! Msg: ${status.getErrorMsgs()}")
                        break
                    case TStatusCode.BINLOG_NOT_FOUND_DB:
                        logger.error("Binlog not found DB! DB: ${context.db}")
                        break
                    case TStatusCode.BINLOG_NOT_FOUND_TABLE:
                        logger.error("Binlog not found table ${table}!")
                        break
                    case TStatusCode.ANALYSIS_ERROR:
                        logger.error("Binlog result analysis error: ${status.getErrorMsgs()}")
                        break
                    default:
                        logger.error("Binlog result is an unexpected code: ${code}")
                        break
                }
            } else {
                logger.error("Invalid TStatus! StatusCode is unset.")
            }
        } else {
            logger.error("Invalid TGetBinlogResult! result: ${result}")
        }

        // step 2: check binlog
        return checkBinlog(binlog, table, update)
    }

    private Boolean checkBeginTxn(TBeginTxnResult result) {
        Boolean isCheckedOK = false

        // step 1: check status
        if (result != null && result.isSetStatus()) {
            TStatus status = result.getStatus()
            if (status.isSetStatusCode()) {
                TStatusCode code = status.getStatusCode()
                switch (code) {
                    case TStatusCode.OK:
                        isCheckedOK = true
                        break
                    case TStatusCode.LABEL_ALREADY_EXISTS:
                        logger.error("Begin transaction label is exist! job status: ${result.getJobStatus()}")
                        break
                    case TStatusCode.ANALYSIS_ERROR:
                        logger.error("Begin transaction analysis error! error massage: ${status.getErrorMsgs()}")
                        break
                    case TStatusCode.INTERNAL_ERROR:
                        logger.error("Begin transaction internal error! error massage: ${status.getErrorMsgs()}")
                        break
                    default:
                        logger.error("Begin transaction result is an unexpected code: ${code}")
                        break
                }
            } else {
                logger.error("Invalid TStatus! StatusCode is unset.")
            }
        } else {
            logger.error("Invalid TBeginTxnResult! result: ${result}")
        }

        // step 2: check and set txnId to context
        if (isCheckedOK && result.isSetTxnId()) {
            logger.info("Begin transaction id is ${result.getTxnId()}")
            context.txnId = result.getTxnId()
            if (result.getSubTxnIds() != null) {
                context.targetSubTxnIds.addAll(result.getSubTxnIds())
            }
            if (context.targetSubTxnIds.size() != context.sourceSubTxnIds.size()) {
                logger.error("source subTxnIds size is not equal to target subTxnIds size, " +
                        "source: ${context.sourceSubTxnIds}, target: ${context.targetSubTxnIds}")
                isCheckedOK = false
            }
            if (isCheckedOK) {
                for (int i = 0; i < context.sourceSubTxnIds.size(); ++i) {
                    context.sourceToTargetSubTxnId.put(context.sourceSubTxnIds[i], context.targetSubTxnIds[i])
                }
            }
            logger.info("sourceToTargetSubTxnId: ${context.sourceToTargetSubTxnId}")
        } else {
            logger.error("Begin transaction txnId is unset!")
            isCheckedOK = false
        }

        // step 3: print result information
        if (isCheckedOK) {
            if (result.isSetDbId()) {
                logger.info("Begin transaction db id is ${result.getDbId()}")
            }
        }
        return isCheckedOK
    }

    private Boolean checkIngestBinlog(TIngestBinlogResult result) {
        Boolean isCheckedOK = false
        if (result != null && result.isSetStatus()) {
            TStatus status = result.getStatus()
            if (status.isSetStatusCode()) {
                TStatusCode code = status.getStatusCode()
                switch (code) {
                    case TStatusCode.OK:
                        isCheckedOK = true
                        break
                    case TStatusCode.NOT_IMPLEMENTED_ERROR:
                        logger.error("Ingest binlog enable feature binlog is false! job status: ${status.getErrorMsgs()}")
                        break
                    case TStatusCode.ANALYSIS_ERROR:
                        logger.error("Ingest binlog analysis error! error massage: ${status.getErrorMsgs()}")
                        break
                    case TStatusCode.RUNTIME_ERROR:
                        logger.error("Ingest binlog runtime error! error massage: ${status.getErrorMsgs()}")
                        break
                    default:
                        logger.error("Ingest binlog result is an unexpected code: ${code}")
                        break
                }
            } else {
                logger.error("Invalid TStatus! StatusCode is unset.")
            }
        } else {
            logger.error("Invalid TIngestBinlogResult! result: ${result}")
        }
        return isCheckedOK
    }

    private Boolean checkCommitTxn(TCommitTxnResult result) {
        Boolean isCheckedOK = false

        // step 1: check status
        if (result != null && result.isSetStatus()) {
            TStatus status = result.getStatus()
            if (status.isSetStatusCode()) {
                TStatusCode code = status.getStatusCode()
                switch (code) {
                    case TStatusCode.OK:
                        isCheckedOK = true
                        break
                    case TStatusCode.PUBLISH_TIMEOUT:
                        logger.error("Commit transaction publish timeout! job status: ${status.getErrorMsgs()}")
                        break
                    case TStatusCode.ANALYSIS_ERROR:
                        logger.error("Commit transaction analysis error! error massage: ${status.getErrorMsgs()}")
                        break
                    case TStatusCode.INTERNAL_ERROR:
                        logger.error("Commit transaction internal error! error massage: ${status.getErrorMsgs()}")
                        break
                    default:
                        logger.error("Commit transaction result is an unexpected code: ${code}")
                        break
                }
            } else {
                logger.error("Invalid TStatus! StatusCode is unset.")
            }
        } else {
            logger.error("Invalid TCommitTxnResult! result: ${result}")
        }

        if (isCheckedOK) {
            logger.info("CommitInfos: ${context.commitInfos}")
            context.commitInfos.clear()
        }

        return isCheckedOK
    }

    Boolean checkRestoreSnapshot(TRestoreSnapshotResult result) {
        Boolean isCheckedOK = false

        // step 1: check status
        if (result != null && result.isSetStatus()) {
            TStatus status = result.getStatus()
            if (status.isSetStatusCode()) {
                TStatusCode code = status.getStatusCode()
                switch (code) {
                    case TStatusCode.OK:
                        isCheckedOK = true
                        break
                    default:
                        logger.error("Restore SnapShot result code is: ${code}")
                        break
                }
            } else {
                logger.error("Invalid TStatus! StatusCode is unset.")
            }
        } else {
            logger.error("Invalid TRestoreSnapshotResult! result: ${result}")
        }

        return isCheckedOK
    }

    Boolean checkGetMasterToken(TGetMasterTokenResult result) {
        Boolean isCheckedOK = false

        // step 1: check status
        if (result != null && result.isSetStatus()) {
            TStatus status = result.getStatus()
            if (status.isSetStatusCode()) {
                TStatusCode code = status.getStatusCode()
                switch (code) {
                    case TStatusCode.OK:
                        isCheckedOK = result.isSetToken()
                        break
                    default:
                        logger.error("Get Master token result code is: ${code}")
                        break
                }
            } else {
                logger.error("Invalid TStatus! StatusCode is unset.")
            }
        } else {
            logger.error("Invalid TGetMasterTokenResult! result: ${result}")
        }

        if (isCheckedOK) {
            context.token = result.getToken()
            logger.info("Token is ${context.token}.")
        }

        return isCheckedOK
    }

    Boolean checkSnapshotFinish(String dbName = null) {
        if (dbName == null) {
            dbName = context.db
        }
        String checkSQL = "SHOW BACKUP FROM ${dbName}"
        def records = suite.sql(checkSQL)
        def allDone = true
        for (def row in records) {
            logger.info("BACKUP row is ${row}")
            String state = (row[3] as String);
            if (state != "FINISHED" && state != "CANCELLED") {
                allDone = false
            }
        }
        allDone
    }

    void waitSnapshotFinish(String dbName = null) {
        int count = 0;
        while (!checkSnapshotFinish(dbName)) {
            if (++count >= 600) {  // 30min
                logger.error('BACKUP task is timeouted')
                throw new Exception("BACKUP task is timeouted after 30mins")
            }
            Thread.sleep(3000)
        }
    }

    String getSnapshotTimestamp(String repoName, String snapshotName) {
        def filterShowSnapshot = { records, name ->
            for (def row in records) {
                logger.info("Snapshot row is ${row}")
                if (row[0] == name && row[1] != "null") {
                    return row
                }
            }
            null
        }

        for (int i = 0; i < 3; ++i) {
            def result = suite.sql "SHOW SNAPSHOT ON ${repoName}"
            def snapshot = filterShowSnapshot(result, snapshotName)
            if (snapshot != null) {
                return snapshot[1].split('\n').last()
            }
            Thread.sleep(3000);
        }
        null
    }

    Boolean checkAllRestoreFinish(String dbName = null) {
        if (dbName == null) {
            dbName = context.db
        }
        String checkSQL = "SHOW RESTORE FROM ${dbName}"
        def records = suite.sql(checkSQL)
        def allDone = true
        for (def row in records) {
            logger.info("Restore row is ${row}")
            String state = row[4]
            if (state != "FINISHED" && state != "CANCELLED") {
                allDone = false
            }
        }
        allDone
    }

    Boolean checkRestoreError(String dbName = null, String message = null) {
        if (dbName == null) {
            dbName = context.db
        }
        String checkSQL = "SHOW RESTORE FROM ${dbName}"
        def records = suite.sql(checkSQL)
        def haveError = false
        def expectMessage = (message == null)
        for (def row in records) {
            logger.info("Restore row is ${row}")
            String mess = row[19]
            haveError = mess != "[OK]"
            if (haveError && message != null) {
                expectMessage = mess.contains(message)
            }
        }
        (haveError && expectMessage)
    }

    void waitRestoreError(String dbName = null, String message = null) {
        int count = 0;
        while (!checkRestoreError(dbName, message)) {
            if (++count >= 600) {  // 30min
                logger.error('RESTORE task is timeouted')
                throw new Exception("RESTORE task is timeouted after 30mins")
            }
            Thread.sleep(3000)
        }
    }

    void waitAllRestoreFinish(String dbName = null) {
        int count = 0;
        while (!checkAllRestoreFinish(dbName)) {
            if (++count >= 600) {  // 30min
                logger.error('RESTORE task is timeouted')
                throw new Exception("RESTORE task is timeouted after 30mins")
            }
            Thread.sleep(3000)
        }
    }

    Boolean checkRestoreFinish() {
        String checkSQL = "SHOW RESTORE FROM TEST_" + context.db
        int size = suite.sql(checkSQL).size()
        logger.info("Now size is ${size}")
        List<Object> row = suite.sql(checkSQL)[size-1]
        logger.info("Now row is ${row}")

        return (row[4] as String) == "FINISHED"
    }

    void waitTargetRestoreFinish() {
        int count = 0;
        while (!checkRestoreFinish()) {
            if (++count >= 600) {  // 30min
                logger.error('target RESTORE task is timeouted')
                throw new Exception("target RESTORE task is timeouted after 30mins")
            }
            Thread.sleep(3000)
        }
    }

    Boolean checkGetSnapshot() {
        TGetSnapshotResult result = context.getSnapshotResult
        Boolean isCheckedOK = false

        // step 1: check status
        if (result != null && result.isSetStatus()) {
            TStatus status = result.getStatus()
            if (status.isSetStatusCode()) {
                TStatusCode code = status.getStatusCode()
                switch (code) {
                    case TStatusCode.OK:
                        if (!result.isSetMeta()) {
                            logger.error("TGetSnapshotResult meta is unset.")
                        } else if (!result.isSetJobInfo()) {
                            logger.error("TGetSnapshotResult job info is unset.")
                        } else if (!result.isSetExpiredAt()) {
                            logger.error("TGetSnapshotResult expiredAt is unset.")
                        } else {
                            isCheckedOK = true
                        }
                        break
                    default:
                        logger.error("Get SnapShot result code is: ${code}")
                        break
                }
            } else {
                logger.error("Invalid TStatus! StatusCode is unset.")
            }
        } else {
            logger.error("Invalid TGetSnapshotResult! result: ${result}")
        }

        return isCheckedOK
    }

    HashMap<Long, BackendClientImpl> getBackendClientsImpl(ccrCluster cluster) throws TTransportException {
        HashMap<Long, BackendClientImpl> clientsMap = new HashMap<Long, BackendClientImpl>()
        String backendSQL = "SHOW PROC '/backends'"
        List<List<Object>> backendInformation
        if (cluster == ccrCluster.SOURCE) {
            backendInformation = suite.sql(backendSQL)
        } else {
            backendInformation = suite.target_sql(backendSQL)
        }
        for (List<Object> row : backendInformation) {
            TNetworkAddress address = new TNetworkAddress(row[1] as String, row[3] as int)
            BackendClientImpl client = new BackendClientImpl(address, row[4] as int)
            clientsMap.put(row[0] as Long, client)
        }
        return clientsMap
    }

    ArrayList<TTabletCommitInfo> copyCommitInfos() {
        return new ArrayList<TTabletCommitInfo>(context.commitInfos)
    }

    ArrayList<TTabletCommitInfo> resetCommitInfos() {
        def info = copyCommitInfos()
        context.commitInfos = new ArrayList<TTabletCommitInfo>()
        return info
    }

    void addCommitInfo(long tabletId, long backendId) {
        context.commitInfos.add(new TTabletCommitInfo(tabletId, backendId))
    }

    Boolean getBackendClients() {
        logger.info("Begin to get backend's maps.")

        // get source backend clients
        try {
            context.sourceBackendClients = getBackendClientsImpl(ccrCluster.SOURCE)
        } catch (TTransportException e) {
            logger.error("Create source cluster backend client fail: ${e.toString()}")
            return false
        }

        // get target backend clients
        try {
            context.targetBackendClients = getBackendClientsImpl(ccrCluster.TARGET)
        } catch (TTransportException e) {
            logger.error("Create target cluster backend client fail: ${e.toString()}")
            return false
        }
        return true
    }

    void closeBackendClients() {
        context.closeBackendClients()
    }

    Boolean getMasterToken() {
        logger.info("Get master token.")
        FrontendClientImpl clientImpl = context.getSourceFrontClient()
        TGetMasterTokenResult result = SyncerUtils.getMasterToken(clientImpl, context)

        return checkGetMasterToken(result)
    }

    Boolean restoreSnapshot(boolean forCCR = false) {
        logger.info("Restore snapshot ${context.labelName}")
        FrontendClientImpl clientImpl = context.getSourceFrontClient()

        // step 1: get master token
        if (!getMasterToken()) {
            logger.error("Get Master error!")
            return false
        }

        // step 1: recode job info
        Gson gson = new Gson()
        Map jsonMap = gson.fromJson(new String(context.getSnapshotResult.getJobInfo()), Map.class)
        getBackendClients()
        jsonMap.put("extra_info", context.genExtraInfo())
        logger.info("json map ${jsonMap}.")
        context.getSnapshotResult.setJobInfo(gson.toJson(jsonMap).getBytes())

        // step 2: restore
        TRestoreSnapshotResult result = SyncerUtils.restoreSnapshot(clientImpl, context, forCCR)
        return checkRestoreSnapshot(result)
    }

    Boolean getSnapshot(String labelName, String table) {
        logger.info("Get snapshot ${labelName}")
        FrontendClientImpl clientImpl = context.getSourceFrontClient()
        context.getSnapshotResult = SyncerUtils.getSnapshot(clientImpl, labelName, table, context)
        context.labelName = labelName
        context.tableName = table
        return checkGetSnapshot()
    }

    Boolean getSourceMeta(String table = "") {
        logger.info("Get source cluster meta")
        String baseSQL = "SHOW PROC '/dbs"
        List<List<Object>> sqlInfo
        if (context.sourceDbId == -1) {
            sqlInfo = suite.sql(baseSQL + "'")
            for (List<Object> row : sqlInfo) {
                String dbName = (row[1] as String)
                if (dbName == context.db) {
                    context.sourceDbId = row[0] as Long
                    break
                }
            }
        }
        if (context.sourceDbId == -1) {
            logger.error("Get ${context.db} db error.")
            return false
        }
        baseSQL += "/" + context.sourceDbId.toString()
        return getMeta(baseSQL, table, context.sourceTableMap, true)
    }

    Boolean getTargetMeta(String table = "") {
        logger.info("Get target cluster meta")
        String baseSQL = "SHOW PROC '/dbs"
        List<List<Object>> sqlInfo
        if (context.targetDbId == -1) {
            sqlInfo = suite.target_sql(baseSQL + "'")
            for (List<Object> row : sqlInfo) {
                String dbName = (row[1] as String)
                if (dbName == "TEST_" + context.db) {
                    context.targetDbId = row[0] as Long
                    break
                }
            }
        }
        if (context.targetDbId == -1) {
            logger.error("Get TEST_${context.db} db error.")
            return false
        }
        baseSQL += "/" + context.targetDbId.toString()
        return getMeta(baseSQL, table, context.targetTableMap, false)
    }

    Boolean getMeta(String baseSql, String table, Map<String, TableMeta> metaMap, Boolean toSrc) {
        def sendSql = { String sqlStmt, Boolean isToSrc -> List<List<Object>>
            if (isToSrc) {
                return suite.sql(sqlStmt + "'")
            } else {
                return suite.target_sql(sqlStmt + "'")
            }
        }

        List<List<Object>> sqlInfo

        // step 1: get target dbId/tableId
        sqlInfo = sendSql.call(baseSql, toSrc)
        if (table == "") {
            for (List<Object> row : sqlInfo) {
                metaMap.put(row[1] as String, new TableMeta(row[0] as long))
            }
        } else {
            for (List<Object> row : sqlInfo) {
                if ((row[1] as String) == table) {
                    metaMap.put(row[1] as String, new TableMeta(row[0] as long))
                    break
                }
            }
        }

        // step 2: get partitionIds
        metaMap.values().forEach {
            def partitionSql = baseSql + "/" + it.id.toString() + "/partitions"
            Map<Long, Long> partitionInfo = Maps.newHashMap()
            sqlInfo = sendSql.call(partitionSql, toSrc)
            for (List<Object> row : sqlInfo) {
                partitionInfo.put(row[0] as Long, row[2] as Long)
            }
            if (partitionInfo.isEmpty()) {
                logger.error("Target cluster get partitions fault.")
                return false
            }

            // step 3: get partitionMetas
            for (Entry<Long, Long> info : partitionInfo) {

                // step 3.1: get partition/indexId
                String partitionSQl = partitionSql + "/" + info.key.toString()
                sqlInfo = sendSql.call(partitionSQl, toSrc)
                if (sqlInfo.isEmpty()) {
                    logger.error("Target cluster partition-${info.key} indexId fault.")
                    return false
                }
                PartitionMeta meta = new PartitionMeta(sqlInfo[0][0] as Long, info.value)

                // step 3.2: get partition/indexId/tabletId
                partitionSQl += "/" + meta.indexId.toString()
                sqlInfo = sendSql.call(partitionSQl, toSrc)
                Map<Long, Long> replicaMap = Maps.newHashMap()
                for (List<Object> row : sqlInfo) {
                    Long tabletId = row[0] as Long
                    if (!meta.tabletMeta.containsKey(tabletId)) {
                        meta.tabletMeta.put(tabletId, new TabletMeta())
                    }
                    meta.tabletMeta[tabletId].replicas.put(row[1] as Long, row[2] as Long)
                }
                if (meta.tabletMeta.isEmpty()) {
                    logger.error("Target cluster get (partitionId/indexId)-(${info.key}/${meta.indexId}) tabletIds fault.")
                    return false
                }

                it.partitionMap.put(info.key, meta)
            }
        }


        logger.info("cluster metadata: ${metaMap}")
        return true
    }

    Boolean checkTargetVersion() {
        logger.info("Check target tablets version")
        return context.targetTableMap.values().every {
            String baseSQL = "SHOW PROC '/dbs/" + context.targetDbId.toString() + "/" +
                    it.id.toString() + "/partitions/"
            return it.partitionMap.every((id, meta) -> {
                String versionSQL = baseSQL + id.toString() + "/" + meta.indexId.toString()
                List<List<Object>> sqlInfo = suite.target_sql(versionSQL + "'")
                for (List<Object> row : sqlInfo) {
                    Long tabletVersion = row[4] as Long
                    if (tabletVersion != meta.version) {
                        logger.error(
                                "Version miss match! partitionId: ${id}, tabletId: ${row[0] as Long}," +
                                " Now version is ${meta.version}, but tablet version is ${tabletVersion}")
                        return false
                    }
                }
                return true
            })
        }
    }

    Boolean getBinlog(String table = "", Boolean update = true) {
        logger.info("Get binlog from source cluster ${context.config.feSourceThriftNetworkAddress}, binlog seq: ${context.seq}")
        FrontendClientImpl clientImpl = context.getSourceFrontClient()
        Long tableId = -1
        if (!table.isEmpty() && context.sourceTableMap.containsKey(table)) {
            tableId = context.sourceTableMap.get(table).id
        }
        TGetBinlogResult result = SyncerUtils.getBinLog(clientImpl, context, table, tableId)
        return checkGetBinlog(table, result, update)
    }

    Boolean beginTxn(String table) {
        logger.info("Begin transaction to target cluster ${context.config.feTargetThriftNetworkAddress}")
        FrontendClientImpl clientImpl = context.getTargetFrontClient()
        Long tableId = -1
        if (context.sourceTableMap.containsKey(table)) {
            tableId = context.targetTableMap.get(table).id
        }
        TBeginTxnResult result = SyncerUtils.beginTxn(clientImpl, context, tableId, context.sourceSubTxnIds.size())
        logger.info("begin txn result: ${result}")
        return checkBeginTxn(result)
    }

    Boolean ingestBinlog(long fakePartitionId = -1, long fakeVersion = -1) {
        logger.info("Begin to ingest binlog.")

        // step 1: Check meta data is valid
        if (!context.metaIsValid()) {
            logger.error("Meta data miss match, src: ${context.sourceTableMap}, target: ${context.targetTableMap}")
            return false
        }

        BinlogData binlogData = context.lastBinlog

        // step 2: Begin ingest binlog
        // step 2.1: ingest each table in meta

        // sub txn id to tablet commit info
        Map<Long, List<TTabletCommitInfo>> subTxnIdToTabletCommitInfos = new HashMap<Long, List<TTabletCommitInfo>>()
        // sub txn id to table id
        Map<Long, Long> subTxnIdToTableId = new HashMap<Long, Long>()

        for (Entry<String, TableMeta> tableInfo : context.sourceTableMap) {
            String tableName = tableInfo.key
            TableMeta srcTableMeta = tableInfo.value
            if (!binlogData.tableRecords.containsKey(srcTableMeta.id)) {
                continue
            }

            PartitionRecords binlogRecords = binlogData.tableRecords.get(srcTableMeta.id)

            TableMeta tarTableMeta = context.targetTableMap.get(tableName)

            Iterator sourcePartitionIter = srcTableMeta.partitionMap.iterator()
            Iterator targetPartitionIter = tarTableMeta.partitionMap.iterator()

            // step 2.2: ingest each partition in the table
            while (sourcePartitionIter.hasNext()) {
                Entry srcPartition = sourcePartitionIter.next()
                Entry tarPartition = targetPartitionIter.next()
                if (!binlogRecords.contains(srcPartition.key)) {
                    continue
                }

                logger.info("Partition records: ${binlogRecords}")
                for (PartitionData partitionRecord : binlogRecords.partitionRecords) {
                    if (partitionRecord.partitionId != srcPartition.key) {
                        continue
                    }
                    logger.info("Partition record: ${partitionRecord}")
                    long txnId = partitionRecord.stid == -1 ? context.txnId : context.sourceToTargetSubTxnId.get(partitionRecord.stid)
                    // step 2.3: ingest each tablet in the partition
                    Iterator srcTabletIter = srcPartition.value.tabletMeta.iterator()
                    Iterator tarTabletIter = tarPartition.value.tabletMeta.iterator()
                    while (srcTabletIter.hasNext()) {
                        Entry srcTabletMap = srcTabletIter.next()
                        Entry tarTabletMap = tarTabletIter.next()
                        TabletMeta srcTabletMeta = srcTabletMap.value
                        TabletMeta tarTabletMeta = tarTabletMap.value

                        Iterator srcReplicaIter = srcTabletMeta.replicas.iterator()
                        Iterator tarReplicaIter = tarTabletMeta.replicas.iterator()
                        while (srcReplicaIter.hasNext()) {
                            Entry srcReplicaMap = srcReplicaIter.next()
                            Entry tarReplicaMap = tarReplicaIter.next()
                            BackendClientImpl srcClient = context.sourceBackendClients.get(srcReplicaMap.value)
                            if (srcClient == null) {
                                logger.error("Can't find src tabletId-${srcReplicaMap.key} -> beId-${srcReplicaMap.value}")
                                return false
                            }
                            BackendClientImpl tarClient = context.targetBackendClients.get(tarReplicaMap.value)
                            if (tarClient == null) {
                                logger.error("Can't find target tabletId-${tarReplicaMap.key} -> beId-${tarReplicaMap.value}")
                                return false
                            }

                            tarPartition.value.version = srcPartition.value.version
                            long partitionId = fakePartitionId == -1 ? tarPartition.key : fakePartitionId
                            long version = fakeVersion == -1 ? partitionRecord.version : fakeVersion

                            TIngestBinlogRequest request = new TIngestBinlogRequest()
                            TUniqueId uid = new TUniqueId(-1, -1)
                            request.setTxnId(txnId)
                            request.setRemoteTabletId(srcTabletMap.key)
                            request.setBinlogVersion(version)
                            request.setRemoteHost(srcClient.address.hostname)
                            request.setRemotePort(srcClient.httpPort.toString())
                            request.setPartitionId(partitionId)
                            request.setLocalTabletId(tarTabletMap.key)
                            request.setLoadId(uid)
                            logger.info("request -> ${request}")
                            TIngestBinlogResult result = tarClient.client.ingestBinlog(request)
                            if (!checkIngestBinlog(result)) {
                                logger.error("Ingest binlog error! result: ${result}")
                                return false
                            }

                            if (context.txnInsert) {
                                List<TTabletCommitInfo> tabletCommitInfos = subTxnIdToTabletCommitInfos.get(txnId)
                                if (tabletCommitInfos == null) {
                                    tabletCommitInfos = new ArrayList<TTabletCommitInfo>()
                                    subTxnIdToTabletCommitInfos.put(txnId, tabletCommitInfos)
                                    subTxnIdToTableId.put(txnId, tarTableMeta.id)
                                }
                                tabletCommitInfos.add(new TTabletCommitInfo(tarTabletMap.key, tarReplicaMap.value))
                            } else {
                                addCommitInfo(tarTabletMap.key, tarReplicaMap.value)
                            }
                        }
                    }
                }
            }
        }

        if (context.txnInsert) {
            for (long sourceSubTxnId : context.sourceSubTxnIds) {
                long subTxnId = context.sourceToTargetSubTxnId.get(sourceSubTxnId)
                List<TTabletCommitInfo> tabletCommitInfos = subTxnIdToTabletCommitInfos.get(subTxnId)
                TSubTxnInfo subTxnInfo = new TSubTxnInfo().setSubTxnId(subTxnId).setTableId(subTxnIdToTableId.get(subTxnId)).setTabletCommitInfos(tabletCommitInfos)
                context.subTxnInfos.add(subTxnInfo)
            }
        }
        return true
    }

    Boolean commitTxn() {
        logger.info("Commit transaction to target cluster ${context.config.feTargetThriftNetworkAddress}, txnId: ${context.txnId}")
        FrontendClientImpl clientImpl = context.getTargetFrontClient()
        TCommitTxnResult result = SyncerUtils.commitTxn(clientImpl, context)
        return checkCommitTxn(result)
    }

    String externalStoragePrefix() {
        String feAddr = "${context.config.feTargetThriftNetworkAddress}"
        int code = feAddr.hashCode();
        String id = ((code < 0) ? -code : code).toString()

        // An expiration time is configured for the prefix 'doris_build_backup_restore/*'
        // to ensure that the data generated by the regression test will be deleted.
        "doris_build_backup_restore/${id}"
    }

    void createS3Repository(String name, boolean readOnly = false, String prefix = null) {
        String ak = suite.getS3AK()
        String sk = suite.getS3SK()
        String endpoint = suite.getS3Endpoint()
        String region = suite.getS3Region()
        String bucket = suite.getS3BucketName()
        prefix = prefix == null ? externalStoragePrefix() : prefix

        suite.try_sql "DROP REPOSITORY `${name}`"
        suite.sql """
        CREATE ${readOnly ? "READ ONLY" : ""} REPOSITORY `${name}`
        WITH S3
        ON LOCATION "s3://${bucket}/${prefix}/${name}"
        PROPERTIES
        (
            "s3.endpoint" = "http://${endpoint}",
            "s3.region" = "${region}",
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}"
        )
            """
    }

    String createS3ValidateRepository(String suiteName, String validateVersion, boolean readOnly = false) {
        S3RepoValidate s3RepoValidate = new S3RepoValidate(suite, validateVersion, context.config)
        String repoName = s3RepoValidate.findMatchingRepoName(suiteName)
        if (repoName == null) {
            String errorMsg = "No matching repository found for ${suiteName}, version: ${validateVersion}"
            logger.error(errorMsg)
            throw new Exception(errorMsg)
        }
        String prefix = "${context.config.validateBackupPrefix}/${s3RepoValidate.version}"
        createS3Repository(repoName, readOnly, prefix)
        return repoName
    }

    void createHdfsRepository(String name, boolean readOnly = false) {
        String hdfsFs = suite.getHdfsFs()
        String hdfsUser = suite.getHdfsUser()
        String dataDir = suite.getHdfsDataDir()
        String prefix = externalStoragePrefix()

        suite.try_sql "DROP REPOSITORY `${name}`"
        suite.sql """
        CREATE REPOSITORY `${name}`
        WITH hdfs
        ON LOCATION "${dataDir}/${prefix}/${name}"
        PROPERTIES
        (
            "fs.defaultFS" = "${hdfsFs}",
            "hadoop.username" = "${hdfsUser}"
        )
        """
    }

    void createS3RepositoryWithRole(String name, boolean readOnly = false) {
        String roleArn = suite.context.config.awsRoleArn
        String externalId = suite.context.config.awsExternalId
        String endpoint = suite.context.config.awsEndpoint
        String region = suite.context.config.awsRegion
        String bucket = suite.context.config.awsBucket
        String prefix = suite.context.config.awsPrefix

        suite.try_sql "DROP REPOSITORY `${name}`"
        suite.sql """
        CREATE ${readOnly ? "READ ONLY" : ""} REPOSITORY `${name}`
        WITH S3
        ON LOCATION "s3://${bucket}/${prefix}/aws_iam_role_p0/${name}"
        PROPERTIES
        (
            "s3.endpoint" = "${endpoint}",
            "s3.region" = "${region}",
            "s3.role_arn" = "${roleArn}",
            "s3.external_id" = "${externalId}"
        )
        """
    }
}
