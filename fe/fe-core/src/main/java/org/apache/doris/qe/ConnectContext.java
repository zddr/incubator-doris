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

package org.apache.doris.qe;

import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.analysis.VariableExpr;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.catalog.Type;
import org.apache.doris.cloud.qe.ComputeGroupException;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.mysql.DummyMysqlChannel;
import org.apache.doris.mysql.MysqlCapability;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.MysqlSslContext;
import org.apache.doris.mysql.ProxyMysqlChannel;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.plsql.Exec;
import org.apache.doris.plsql.executor.PlSqlOperation;
import org.apache.doris.plugin.AuditEvent.AuditEventBuilder;
import org.apache.doris.resource.Tag;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.resource.computegroup.ComputeGroupMgr;
import org.apache.doris.service.arrowflight.results.FlightSqlChannel;
import org.apache.doris.service.arrowflight.results.FlightSqlEndpointsLocation;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Histogram;
import org.apache.doris.system.Backend;
import org.apache.doris.task.LoadTaskInfo;
import org.apache.doris.thrift.TResultSinkType;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionEntry;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.xnio.StreamConnection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;


// When one client connect in, we create a connect context for it.
// We store session information here. Meanwhile ConnectScheduler all
// connect with its connection id.
// Use `volatile` to make the reference change atomic.
public class ConnectContext {
    private static final Logger LOG = LogManager.getLogger(ConnectContext.class);
    protected static ThreadLocal<ConnectContext> threadLocalInfo = new ThreadLocal<>();

    private static final String SSL_PROTOCOL = "TLS";

    public enum ConnectType {
        MYSQL,
        ARROW_FLIGHT_SQL
    }

    protected volatile ConnectType connectType;
    // set this id before analyze
    protected volatile long stmtId;
    protected volatile long forwardedStmtId;

    // set for http_stream
    protected volatile TUniqueId loadId;
    protected volatile long backendId;
    // range [Integer.MIN_VALUE, Integer.MAX_VALUE]
    protected int preparedStmtId = Integer.MIN_VALUE;
    protected volatile LoadTaskInfo streamLoadInfo;

    protected volatile TUniqueId queryId = null;
    // only be active one time. tell Coordinator to regenerate instance ids for certain query(when retry).
    protected volatile TUniqueId needRegenerateInstanceId = null;
    protected volatile AtomicInteger instanceIdGenerator = new AtomicInteger();
    protected volatile String traceId;
    protected volatile TUniqueId lastQueryId = null;
    // id for this connection
    protected volatile int connectionId;
    // Timestamp when the connection is make
    protected volatile long loginTime;
    // for arrow flight
    protected volatile String peerIdentity;
    private final Map<String, String> preparedQuerys = new HashMap<>();
    private String runningQuery;
    private final List<FlightSqlEndpointsLocation> flightSqlEndpointsLocations = Lists.newArrayList();
    private boolean returnResultFromLocal = true;
    // mysql net
    protected volatile MysqlChannel mysqlChannel;
    // state
    protected volatile QueryState state;
    protected volatile long returnRows;
    // the protocol capability which server say it can support
    protected volatile MysqlCapability serverCapability;
    // the protocol capability after server and client negotiate
    protected volatile MysqlCapability capability;
    // Indicate if this client is killed.
    protected volatile boolean isKilled;
    // Db
    protected volatile String currentDb = "";
    protected volatile long currentDbId = -1;
    // Transaction
    protected volatile TransactionEntry txnEntry = null;
    // used for ShowSqlAction which don't allow a user account
    protected volatile boolean noAuth = false;
    // LDAP authenticated but the Doris account does not exist,
    // set the flag, and the user login Doris as Temporary user.
    protected volatile boolean isTempUser = false;
    // username@host combination for the Doris account
    // that the server used to authenticate the current client.
    // In other word, currentUserIdentity is the entry that matched in Doris auth table.
    // This account determines user's access privileges.
    protected volatile UserIdentity currentUserIdentity;
    // Variables belong to this session.
    protected volatile SessionVariable sessionVariable;
    // Store user variable in this connection
    private Map<String, LiteralExpr> userVars = new HashMap<>();
    // Scheduler this connection belongs to
    protected volatile ConnectScheduler connectScheduler;
    // Executor
    protected volatile StmtExecutor executor;
    // Command this connection is processing.
    protected volatile MysqlCommand command;
    // Timestamp in millisecond last command starts at
    protected volatile long startTime;
    // Cache thread info for this connection.
    protected volatile ThreadInfo threadInfo;

    // Catalog: put catalog here is convenient for unit test,
    // because catalog is singleton, hard to mock
    protected Env env;
    protected String defaultCatalog = InternalCatalog.INTERNAL_CATALOG_NAME;
    protected boolean isSend;

    // record last used database of every catalog
    private final Map<String, String> lastDBOfCatalog = Maps.newConcurrentMap();

    protected AuditEventBuilder auditEventBuilder = new AuditEventBuilder();

    protected String remoteIP;

    // This is used to statistic the current query details.
    // This property will only be set when the query starts to execute.
    // So in the query planning stage, do not use any value in this attribute.
    protected QueryDetail queryDetail = null;

    // cloud cluster name
    protected volatile String cloudCluster = null;

    // If set to true, the nondeterministic function will not be rewrote to constant.
    private boolean notEvalNondeterministicFunction = false;
    // The compute group tag is used to limit the node resources that the user can use for query.
    // The default is empty, that is, unlimited.
    // This property is obtained from UserProperty when the client connection is created.
    // Only when the connection is created again, the new resource tags will be retrieved from the UserProperty
    private ComputeGroup computeGroup = null;

    private PlSqlOperation plSqlOperation = null;

    private String sqlHash;

    private JSONObject minidump = null;

    // The FE ip current connected
    private String currentConnectedFEIp = "";

    private InsertResult insertResult;

    private SessionContext sessionContext;


    // This context is used for SSL connection between server and mysql client.
    private final MysqlSslContext mysqlSslContext = new MysqlSslContext(SSL_PROTOCOL);

    private StatsErrorEstimator statsErrorEstimator;

    private List<Map<String, String>> resultAttachedInfo = Lists.newArrayList();

    private String workloadGroupName = "";
    private boolean isGroupCommit;

    private TResultSinkType resultSinkType = TResultSinkType.MYSQL_PROTOCAL;

    private Map<String, Set<String>> dbToTempTableNamesMap = new HashMap<>();

    // unique session id in the doris cluster
    private String sessionId;

    // internal call like `insert overwrite` need skipAuth
    // For example, `insert overwrite` only requires load permission,
    // but the internal implementation will call the logic of `AlterTable`.
    // In this case, `skipAuth` needs to be set to `true` to skip the permission check of `AlterTable`
    private boolean skipAuth = false;
    private Exec exec;
    private boolean runProcedure = false;

    // isProxy used for forward request from other FE and used in one thread
    // it's default thread-safe
    private boolean isProxy = false;

    @Getter
    @Setter
    private ByteBuffer prepareExecuteBuffer;

    private MysqlHandshakePacket mysqlHandshakePacket;

    public void setUserQueryTimeout(int queryTimeout) {
        if (queryTimeout > 0) {
            sessionVariable.setQueryTimeoutS(queryTimeout);
        }
    }

    public void setUserInsertTimeout(int insertTimeout) {
        if (insertTimeout > 0) {
            sessionVariable.setInsertTimeoutS(insertTimeout);
        }
    }

    private StatementContext statementContext;

    // new planner
    private Map<String, PreparedStatementContext> preparedStatementContextMap = Maps.newHashMap();

    private Map<String, ColumnStatistic> totalColumnStatisticMap = new HashMap<>();

    public Map<String, ColumnStatistic> getTotalColumnStatisticMap() {
        return totalColumnStatisticMap;
    }

    public void setTotalColumnStatisticMap(Map<String, ColumnStatistic> totalColumnStatisticMap) {
        this.totalColumnStatisticMap = totalColumnStatisticMap;
    }

    private Map<String, Histogram> totalHistogramMap = new HashMap<>();

    public Map<String, Histogram> getTotalHistogramMap() {
        return totalHistogramMap;
    }

    public void setTotalHistogramMap(Map<String, Histogram> totalHistogramMap) {
        this.totalHistogramMap = totalHistogramMap;
    }

    public SessionContext getSessionContext() {
        return sessionContext;
    }

    public MysqlSslContext getMysqlSslContext() {
        return mysqlSslContext;
    }

    public TResultSinkType getResultSinkType() {
        return resultSinkType;
    }

    public void setOrUpdateInsertResult(long txnId, String label, String db, String tbl,
            TransactionStatus txnStatus, long loadedRows, int filteredRows) {
        if (isTxnModel() && insertResult != null) {
            insertResult.updateResult(txnStatus, loadedRows, filteredRows);
        } else {
            insertResult = new InsertResult(txnId, label, db, Util.getTempTableDisplayName(tbl),
                txnStatus, loadedRows, filteredRows);
        }
    }

    public InsertResult getInsertResult() {
        return insertResult;
    }

    public static ConnectContext get() {
        return threadLocalInfo.get();
    }

    public static void remove() {
        threadLocalInfo.remove();
    }

    public void setIsSend(boolean isSend) {
        this.isSend = isSend;
    }

    public boolean isSend() {
        return this.isSend;
    }

    public void addLastDBOfCatalog(String catalog, String db) {
        lastDBOfCatalog.put(catalog, db);
    }

    public String getLastDBOfCatalog(String catalog) {
        return lastDBOfCatalog.get(catalog);
    }

    public String removeLastDBOfCatalog(String catalog) {
        return lastDBOfCatalog.get(catalog);
    }

    // Used by COM_RESET_CONNECTION
    public void clearLastDBOfCatalog() {
        lastDBOfCatalog.clear();
    }

    public void setNotEvalNondeterministicFunction(boolean notEvalNondeterministicFunction) {
        this.notEvalNondeterministicFunction = notEvalNondeterministicFunction;
    }

    public boolean notEvalNondeterministicFunction() {
        return notEvalNondeterministicFunction;
    }

    public ConnectType getConnectType() {
        return connectType;
    }

    public void init() {
        state = new QueryState();
        returnRows = 0;
        isKilled = false;
        sessionVariable = VariableMgr.newSessionVariable();
        userVars = new HashMap<>();
        command = MysqlCommand.COM_SLEEP;
        if (Config.use_fuzzy_session_variable) {
            sessionVariable.initFuzzyModeVariables();
        }

        sessionId = UUID.randomUUID().toString();
        if (!FeConstants.runningUnitTest) {
            Env.getCurrentEnv().registerSessionInfo(sessionId);
        }
    }

    public ConnectContext() {
        this(null);
    }

    public ConnectContext(StreamConnection connection) {
        this(connection, false);
    }

    public ConnectContext(StreamConnection connection, boolean isProxy, String sessionId) {
        this(connection, isProxy);
        // used for binding new created temporary table with its original session
        this.sessionId = sessionId;
    }

    public ConnectContext(StreamConnection connection, boolean isProxy) {
        connectType = ConnectType.MYSQL;
        serverCapability = MysqlCapability.DEFAULT_CAPABILITY;
        if (connection != null) {
            mysqlChannel = new MysqlChannel(connection, this);
        } else if (isProxy) {
            mysqlChannel = new ProxyMysqlChannel();
            this.isProxy = isProxy;
        } else {
            mysqlChannel = new DummyMysqlChannel();
        }
        init();
    }

    public ConnectContext cloneContext() {
        ConnectContext context = new ConnectContext();
        context.mysqlChannel = mysqlChannel;
        context.setSessionVariable(VariableMgr.cloneSessionVariable(sessionVariable)); // deep copy
        context.setEnv(env);
        context.setDatabase(currentDb);
        context.setCurrentUserIdentity(currentUserIdentity);
        context.setProcedureExec(exec);
        return context;
    }

    public boolean isTxnModel() {
        return txnEntry != null && txnEntry.isTxnModel();
    }

    public boolean isInsertValuesTxnIniting() {
        return txnEntry != null && txnEntry.isInsertValuesTxnIniting();
    }

    public void addPreparedStatementContext(String stmtName, PreparedStatementContext ctx) throws UserException {
        if (!sessionVariable.enableServeSidePreparedStatement) {
            throw new UserException("Failed to do prepared command, server side prepared statement is disabled");
        }
        if (this.preparedStatementContextMap.size() > sessionVariable.maxPreparedStmtCount) {
            throw new UserException("Failed to create a server prepared statement"
                    + " possibly because there are too many active prepared statements on server already."
                    + "set max_prepared_stmt_count with larger number than " + sessionVariable.maxPreparedStmtCount);
        }
        this.preparedStatementContextMap.put(stmtName, ctx);
        incPreparedStmtId();
    }

    public void removePrepareStmt(String stmtName) {
        this.preparedStatementContextMap.remove(stmtName);
    }

    public PreparedStatementContext getPreparedStementContext(String stmtName) {
        return this.preparedStatementContextMap.get(stmtName);
    }

    public void closeTxn() {
        if (isTxnModel()) {
            try {
                txnEntry.abortTransaction();
            } catch (Exception e) {
                LOG.error("db: {}, txnId: {}, rollback error.", currentDb,
                        txnEntry.getTransactionId(), e);
            }
            txnEntry = null;
        }
    }

    public long getStmtId() {
        return stmtId;
    }

    public long getPreparedStmtId() {
        return preparedStmtId;
    }

    public void incPreparedStmtId() {
        ++preparedStmtId;
    }

    public long getBackendId() {
        return backendId;
    }

    public void setBackendId(long backendId) {
        this.backendId = backendId;
    }

    public TUniqueId getLoadId() {
        return loadId;
    }

    public void setLoadId(TUniqueId loadId) {
        this.loadId = loadId;
    }

    public void setStreamLoadInfo(LoadTaskInfo streamLoadInfo) {
        this.streamLoadInfo = streamLoadInfo;
    }

    public LoadTaskInfo getStreamLoadInfo() {
        return streamLoadInfo;
    }

    public void setStmtId(long stmtId) {
        this.stmtId = stmtId;
    }

    public long getForwardedStmtId() {
        return forwardedStmtId;
    }

    public void setForwardedStmtId(long forwardedStmtId) {
        this.forwardedStmtId = forwardedStmtId;
    }

    public String getRemoteIP() {
        return remoteIP;
    }

    public void setRemoteIP(String remoteIP) {
        this.remoteIP = remoteIP;
    }

    public void setQueryDetail(QueryDetail queryDetail) {
        this.queryDetail = queryDetail;
    }

    public QueryDetail getQueryDetail() {
        return queryDetail;
    }

    public AuditEventBuilder getAuditEventBuilder() {
        return auditEventBuilder;
    }

    public void setThreadLocalInfo() {
        threadLocalInfo.set(this);
    }

    public long getCurrentDbId() {
        return currentDbId;
    }

    public TransactionEntry getTxnEntry() {
        return txnEntry;
    }

    public void setTxnEntry(TransactionEntry txnEntry) {
        this.txnEntry = txnEntry;
    }

    public void setEnv(Env env) {
        this.env = env;
        defaultCatalog = env.getInternalCatalog().getName();
    }

    public void setUserVar(SetVar setVar) {
        userVars.put(setVar.getVariable().toLowerCase(), setVar.getResult());
    }

    public void setUserVar(String name, LiteralExpr value) {
        userVars.put(name.toLowerCase(), value);
    }

    public @Nullable Literal getLiteralForUserVar(String varName) {
        varName = varName.toLowerCase();
        if (userVars.containsKey(varName)) {
            LiteralExpr literalExpr = userVars.get(varName);
            if (literalExpr instanceof BoolLiteral) {
                return Literal.of(((BoolLiteral) literalExpr).getValue());
            } else if (literalExpr instanceof IntLiteral) {
                return Literal.of(((IntLiteral) literalExpr).getValue());
            } else if (literalExpr instanceof FloatLiteral) {
                return Literal.of(((FloatLiteral) literalExpr).getValue());
            } else if (literalExpr instanceof DecimalLiteral) {
                return Literal.of(((DecimalLiteral) literalExpr).getValue());
            } else if (literalExpr instanceof StringLiteral) {
                return Literal.of(((StringLiteral) literalExpr).getValue());
            } else if (literalExpr instanceof NullLiteral) {
                return Literal.of(null);
            } else {
                return Literal.of(literalExpr.getStringValue());
            }
        } else {
            // If there are no such user defined var, just return the NULL value.
            return Literal.of(null);
        }
    }

    // Get variable value through variable name, used to satisfy statement like `SELECT @@comment_version`
    public void fillValueForUserDefinedVar(VariableExpr desc) {
        String varName = desc.getName().toLowerCase();
        if (userVars.containsKey(varName)) {
            LiteralExpr literalExpr = userVars.get(varName);
            desc.setType(literalExpr.getType());
            if (literalExpr instanceof BoolLiteral) {
                desc.setBoolValue(((BoolLiteral) literalExpr).getValue());
            } else if (literalExpr instanceof IntLiteral) {
                desc.setIntValue(((IntLiteral) literalExpr).getValue());
            } else if (literalExpr instanceof FloatLiteral) {
                desc.setFloatValue(((FloatLiteral) literalExpr).getValue());
            } else if (literalExpr instanceof DecimalLiteral) {
                desc.setDecimalValue(((DecimalLiteral) literalExpr).getValue());
            } else if (literalExpr instanceof StringLiteral) {
                desc.setStringValue(((StringLiteral) literalExpr).getValue());
            } else if (literalExpr instanceof NullLiteral) {
                desc.setType(Type.NULL);
                desc.setIsNull();
            } else {
                desc.setType(Type.VARCHAR);
                desc.setStringValue(literalExpr.getStringValue());
            }
        } else {
            // If there are no such user defined var, just fill the NULL value.
            desc.setType(Type.NULL);
            desc.setIsNull();
        }
    }

    public Env getEnv() {
        return env;
    }

    public boolean getNoAuth() {
        return noAuth;
    }

    public void setNoAuth(boolean noAuth) {
        this.noAuth = noAuth;
    }

    public String getQualifiedUser() {
        return currentUserIdentity == null ? null : currentUserIdentity.getQualifiedUser();
    }

    public boolean getIsTempUser() {
        return isTempUser;
    }

    public void setIsTempUser(boolean isTempUser) {
        this.isTempUser = isTempUser;
    }

    public UserIdentity getCurrentUserIdentity() {
        return currentUserIdentity;
    }

    // used for select user(), select session_user();
    // return string similar with user@127.0.0.1
    public String getUserWithLoginRemoteIpString() {
        return UserIdentity.createAnalyzedUserIdentWithIp(getQualifiedUser(), remoteIP).toString();
    }

    public void setCurrentUserIdentity(UserIdentity currentUserIdentity) {
        this.currentUserIdentity = currentUserIdentity;
    }

    public SessionVariable getSessionVariable() {
        return sessionVariable;
    }

    public void setSessionVariable(SessionVariable sessionVariable) {
        this.sessionVariable = sessionVariable;
    }

    public ConnectScheduler getConnectScheduler() {
        return connectScheduler;
    }

    public void setConnectScheduler(ConnectScheduler connectScheduler) {
        this.connectScheduler = connectScheduler;
    }

    public MysqlCommand getCommand() {
        return command;
    }

    public void setCommand(MysqlCommand command) {
        this.command = command;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime() {
        startTime = System.currentTimeMillis();
        returnRows = 0;
    }

    public void updateReturnRows(int returnRows) {
        this.returnRows += returnRows;
    }

    public long getReturnRows() {
        return returnRows;
    }

    public void resetReturnRows() {
        returnRows = 0;
    }

    public int getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(int connectionId) {
        this.connectionId = connectionId;
    }

    public void resetLoginTime() {
        this.loginTime = System.currentTimeMillis();
    }

    public void addPreparedQuery(String preparedStatementId, String preparedQuery) {
        preparedQuerys.put(preparedStatementId, preparedQuery);
    }

    public String getPreparedQuery(String preparedStatementId) {
        return preparedQuerys.get(preparedStatementId);
    }

    public void removePreparedQuery(String preparedStatementId) {
        preparedQuerys.remove(preparedStatementId);
    }

    public void setRunningQuery(String runningQuery) {
        this.runningQuery = runningQuery;
    }

    public String getRunningQuery() {
        return runningQuery;
    }

    public void addFlightSqlEndpointsLocation(FlightSqlEndpointsLocation flightSqlEndpointsLocation) {
        this.flightSqlEndpointsLocations.add(flightSqlEndpointsLocation);
    }

    public List<FlightSqlEndpointsLocation> getFlightSqlEndpointsLocations() {
        return flightSqlEndpointsLocations;
    }

    public void clearFlightSqlEndpointsLocations() {
        flightSqlEndpointsLocations.clear();
    }

    public void setReturnResultFromLocal(boolean returnResultFromLocal) {
        this.returnResultFromLocal = returnResultFromLocal;
    }

    public boolean isReturnResultFromLocal() {
        return returnResultFromLocal;
    }

    public String getPeerIdentity() {
        return peerIdentity;
    }

    public FlightSqlChannel getFlightSqlChannel() {
        throw new RuntimeException("getFlightSqlChannel not in flight sql connection");
    }

    public MysqlChannel getMysqlChannel() {
        return mysqlChannel;
    }

    public String getClientIP() {
        return getMysqlChannel().getRemoteHostPortString();
    }

    public QueryState getState() {
        return state;
    }

    public void setState(QueryState state) {
        this.state = state;
    }

    public MysqlCapability getCapability() {
        return capability;
    }

    public void setCapability(MysqlCapability capability) {
        this.capability = capability;
    }

    public MysqlCapability getServerCapability() {
        return serverCapability;
    }

    public String getDefaultCatalog() {
        return defaultCatalog;
    }

    public CatalogIf getCurrentCatalog() {
        // defaultCatalog is switched by SwitchStmt, so we don't need to check to exist of catalog.
        return getCatalog(defaultCatalog);
    }

    public String getSessionId() {
        return sessionId;
    }

    /**
     * Maybe return when catalogName is not exist. So need to check nullable.
     */
    public CatalogIf getCatalog(String catalogName) {
        String realCatalogName = catalogName == null ? defaultCatalog : catalogName;
        if (env == null) {
            return Env.getCurrentEnv().getCatalogMgr().getCatalog(realCatalogName);
        }
        return env.getCatalogMgr().getCatalog(realCatalogName);
    }

    public FunctionRegistry getFunctionRegistry() {
        if (env == null) {
            return Env.getCurrentEnv().getFunctionRegistry();
        }
        return env.getFunctionRegistry();
    }

    public void changeDefaultCatalog(String catalogName) {
        defaultCatalog = catalogName;
        currentDb = "";
        currentDbId = -1;
    }

    public String getDatabase() {
        return currentDb;
    }

    public void setDatabase(String db) {
        currentDb = db;
        Optional<DatabaseIf> dbInstance = getCurrentCatalog().getDb(db);
        currentDbId = dbInstance.map(DatabaseIf::getId).orElse(-1L);
    }

    public void setExecutor(StmtExecutor executor) {
        this.executor = executor;
    }

    public StmtExecutor getExecutor() {
        return executor;
    }

    public void clear() {
        executor = null;
        statementContext = null;
    }

    public PlSqlOperation getPlSqlOperation() {
        if (plSqlOperation == null) {
            plSqlOperation = new PlSqlOperation();
        }
        return plSqlOperation;
    }

    protected void closeChannel() {
        if (mysqlChannel != null) {
            mysqlChannel.close();
        }
    }

    public void cleanup() {
        closeChannel();
        threadLocalInfo.remove();
        returnRows = 0;
        deleteTempTable();
        Env.getCurrentEnv().unregisterSessionInfo(this.sessionId);
    }

    protected void deleteTempTable() {
        // only delete temporary table in its creating session, not proxy session in master fe
        if (isProxy) {
            return;
        }

        // if current fe is master, delete temporary table directly
        if (Env.getCurrentEnv().isMaster()) {
            for (String dbName : dbToTempTableNamesMap.keySet()) {
                Database db = Env.getCurrentEnv().getInternalCatalog().getDb(dbName).get();
                for (String tableName : dbToTempTableNamesMap.get(dbName)) {
                    LOG.info("try to drop temporary table: {}.{}", dbName, tableName);
                    try {
                        Env.getCurrentEnv().getInternalCatalog()
                            .dropTableWithoutCheck(db, db.getTable(tableName).get(), false, true);
                    } catch (DdlException e) {
                        LOG.error("drop temporary table error: {}.{}", dbName, tableName, e);
                    }
                }
            }
        } else {
            // forward to master fe to drop table
            RedirectStatus redirectStatus = new RedirectStatus(true, false);
            for (String dbName : dbToTempTableNamesMap.keySet()) {
                for (String tableName : dbToTempTableNamesMap.get(dbName)) {
                    LOG.info("request to delete temporary table: {}.{}", dbName, tableName);
                    String dropTableSql = String.format("drop table `%s`", tableName);
                    OriginStatement originStmt = new OriginStatement(dropTableSql, 0);
                    MasterOpExecutor masterOpExecutor = new MasterOpExecutor(originStmt, this, redirectStatus, false);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("need to transfer to Master. stmt: {}", this.getStmtId());
                    }
                    try {
                        masterOpExecutor.execute();
                    } catch (Exception e) {
                        LOG.error("master FE drop temporary table error: db: {}, table: {}", dbName, tableName, e);
                    }
                }
            }
        }
    }

    public boolean isKilled() {
        return isKilled;
    }

    // Set kill flag to true;
    public void setKilled() {
        isKilled = true;
    }

    public void setQueryId(TUniqueId queryId) {
        if (this.queryId != null) {
            this.lastQueryId = this.queryId.deepCopy();
        }
        this.queryId = queryId;
        if (connectScheduler != null && !Strings.isNullOrEmpty(traceId)) {
            connectScheduler.getConnectPoolMgr().putTraceId2QueryId(traceId, queryId);
        }
    }

    public void resetQueryId() {
        if (this.queryId != null) {
            this.lastQueryId = this.queryId.deepCopy();
        }
        this.queryId = null;
    }

    public void setNeedRegenerateInstanceId(TUniqueId needRegenerateInstanceId) {
        this.needRegenerateInstanceId = needRegenerateInstanceId;
    }

    public void setTraceId(String traceId) {
        // When traceId is set, we need to remove the old traceId from connectScheduler.
        if (connectScheduler != null) {
            connectScheduler.removeOldTraceId(this.traceId);
        }
        this.traceId = traceId;
    }

    public String traceId() {
        return traceId;
    }

    public TUniqueId queryId() {
        return queryId;
    }

    public TUniqueId getLastQueryId() {
        return lastQueryId;
    }

    public TUniqueId nextInstanceId() {
        if (loadId != null) {
            return new TUniqueId(loadId.hi, loadId.lo + instanceIdGenerator.incrementAndGet());
        } else {
            return new TUniqueId(queryId.hi, queryId.lo + instanceIdGenerator.incrementAndGet());
        }
    }

    public boolean consumeNeedRegenerateQueryId() {
        if (needRegenerateInstanceId == queryId) {
            needRegenerateInstanceId = null; // consume it
            return true;
        }
        return false;
    }

    public String getSqlHash() {
        return sqlHash;
    }

    public void setSqlHash(String sqlHash) {
        this.sqlHash = sqlHash;
    }

    public JSONObject getMinidump() {
        return minidump;
    }

    public void setMinidump(JSONObject minidump) {
        this.minidump = minidump;
    }

    public StatementContext getStatementContext() {
        return statementContext;
    }

    public void setStatementContext(StatementContext statementContext) {
        this.statementContext = statementContext;
    }

    public void setResultSinkType(TResultSinkType resultSinkType) {
        this.resultSinkType = resultSinkType;
    }

    public String getRemoteHostPortString() {
        return getMysqlChannel().getRemoteHostPortString();
    }

    // kill operation with no protect.
    public void kill(boolean killConnection) {
        LOG.warn("kill query from {}, kill {} connection: {}", getRemoteHostPortString(), getConnectType(),
                killConnection);

        if (killConnection) {
            isKilled = true;
            // Close channel to break connection with client
            closeChannel();
        }
        // Now, cancel running query.
        cancelQuery(new Status(TStatusCode.CANCELLED, "cancel query by user from " + getRemoteHostPortString()));
    }

    // kill operation with no protect by timeout.
    private void killByTimeout(boolean killConnection) {
        if (killConnection) {
            LOG.warn("kill wait timeout connection, connection type: {}, connectionId: {}, remote: {}, "
                            + "wait timeout: {}",
                    getConnectType(), connectionId, getRemoteHostPortString(), sessionVariable.getWaitTimeoutS());
            isKilled = true;
            // Close channel to break connection with client
            closeChannel();
        }
        // Now, cancel running query.
        // cancelQuery by time out
        StmtExecutor executorRef = executor;
        if (executorRef != null) {
            LOG.warn("kill time out query, remote: {}, at the same time kill connection is {},"
                            + " connection type: {}, connectionId: {}",
                    getRemoteHostPortString(), killConnection,
                    getConnectType(), connectionId);
            executorRef.cancel(new Status(TStatusCode.TIMEOUT,
                    "query is timeout, killed by timeout checker"));
        }
    }

    public void cancelQuery(Status cancelReason) {
        StmtExecutor executorRef = executor;
        if (executorRef != null) {
            executorRef.cancel(cancelReason);
        }
    }

    public void checkTimeout(long now) {
        if (startTime <= 0) {
            return;
        }

        long delta = now - startTime;
        boolean killFlag = false;
        boolean killConnection = false;
        if (command == MysqlCommand.COM_SLEEP) {
            if (delta > sessionVariable.getWaitTimeoutS() * 1000L) {
                // Need kill this connection.
                killFlag = true;
                killConnection = true;
            }
        } else {
            String timeoutTag = "query";
            // insert stmt particularly
            if (executor != null && executor.isSyncLoadKindStmt()) {
                timeoutTag = "insert";
            }
            // to ms
            long timeout = getExecTimeoutS() * 1000L;
            if (delta > timeout) {
                LOG.warn("kill {} timeout, remote: {}, query timeout: {}ms, query id: {}",
                        timeoutTag, getRemoteHostPortString(), timeout, DebugUtil.printId(queryId));
                killFlag = true;
            }
        }

        if (killFlag) {
            killByTimeout(killConnection);
        }
    }

    // Helper to dump connection information.
    public ThreadInfo toThreadInfo(boolean isFull) {
        if (threadInfo == null) {
            threadInfo = new ThreadInfo();
        }
        threadInfo.isFull = isFull;
        return threadInfo;
    }

    public boolean isSetComputeGroup() {
        return computeGroup != null;
    }

    public void setComputeGroup(ComputeGroup computeGroup) {
        this.computeGroup = computeGroup;
    }

    public void setCurrentConnectedFEIp(String ip) {
        this.currentConnectedFEIp = ip;
    }

    public String getCurrentConnectedFEIp() {
        return currentConnectedFEIp;
    }

    /**
     * We calculate and get the exact execution timeout here, rather than setting
     * execution timeout in many other places.
     *
     * @return exact execution timeout
     */
    public int getExecTimeoutS() {
        if (executor != null && executor.isSyncLoadKindStmt()) {
            // particular for insert stmt, we can expand other type of timeout in the same way
            return Math.max(getInsertTimeoutS(), getQueryTimeoutS());
        } else {
            // normal query stmt
            return getQueryTimeoutS();
        }
    }

    /**
     * First, retrieve from the user's attributes. If not, retrieve from the session variable
     *
     * @return insertTimeoutS
     */
    public int getInsertTimeoutS() {
        int userInsertTimeout = getInsertTimeoutSFromProperty();
        if (userInsertTimeout > 0) {
            return userInsertTimeout;
        }
        return sessionVariable.getInsertTimeoutS();
    }

    private int getInsertTimeoutSFromProperty() {
        if (env == null || env.getAuth() == null || StringUtils.isEmpty(getQualifiedUser())) {
            return 0;
        }
        return env.getAuth().getInsertTimeout(getQualifiedUser());
    }

    /**
     * First, retrieve from the user's attributes. If not, retrieve from the session variable
     *
     * @return queryTimeoutS
     */
    public int getQueryTimeoutS() {
        int userQueryTimeout = getQueryTimeoutSFromProperty();
        if (userQueryTimeout > 0) {
            return userQueryTimeout;
        }
        return sessionVariable.getQueryTimeoutS();
    }

    private int getQueryTimeoutSFromProperty() {
        if (env == null || env.getAuth() == null || StringUtils.isEmpty(getQualifiedUser())) {
            return 0;
        }
        return env.getAuth().getQueryTimeout(getQualifiedUser());
    }

    /**
     * First, retrieve from the user's attributes. If not, retrieve from the session variable
     *
     * @return maxExecMemByte
     */
    public long getMaxExecMemByte() {
        long userLimit = getMaxExecMemByteFromProperty();
        if (userLimit > 0) {
            return userLimit;
        }
        return sessionVariable.getMaxExecMemByte();
    }

    private long getMaxExecMemByteFromProperty() {
        if (env == null || env.getAuth() == null || StringUtils.isEmpty(getQualifiedUser())) {
            return 0L;
        }
        return env.getAuth().getExecMemLimit(getQualifiedUser());
    }

    public void addResultAttachedInfo(Map<String, String> resultAttachedInfo) {
        this.resultAttachedInfo.add(resultAttachedInfo);
    }

    public List<Map<String, String>> getResultAttachedInfo() {
        return resultAttachedInfo;
    }

    public class ThreadInfo {
        public boolean isFull;

        public List<String> toRow(int connId, long nowMs, Optional<String> timeZone) {
            List<String> row = Lists.newArrayList();
            if (connId == connectionId) {
                row.add("Yes");
            } else {
                row.add("No");
            }
            row.add("" + connectionId);
            row.add(ClusterNamespace.getNameFromFullName(getQualifiedUser()));
            row.add(getRemoteHostPortString());
            if (timeZone.isPresent()) {
                row.add(TimeUtils.longToTimeStringWithTimeZone(loginTime, timeZone.get()));
            } else {
                row.add(TimeUtils.longToTimeString(loginTime));
            }
            row.add(defaultCatalog);
            row.add(ClusterNamespace.getNameFromFullName(currentDb));
            row.add(command.toString());
            row.add("" + (nowMs - startTime) / 1000);
            row.add(state.toString());
            row.add(DebugUtil.printId(queryId));
            row.add(Strings.nullToEmpty(traceId));
            if (state.getStateType() == QueryState.MysqlStateType.ERR) {
                row.add(state.getErrorMessage());
            } else if (executor != null) {
                String sql = executor.getOriginStmtInString();
                if (!isFull) {
                    sql = sql.substring(0, Math.min(sql.length(), 100));
                }
                row.add(sql);
            } else {
                row.add("");
            }

            row.add(Env.getCurrentEnv().getSelfNode().getHost());
            if (cloudCluster == null) {
                row.add("NULL");
            } else {
                row.add(cloudCluster);
            }
            return row;
        }
    }

    public void startAcceptQuery(ConnectProcessor connectProcessor) {
        mysqlChannel.startAcceptQuery(this, connectProcessor);
    }

    public void suspendAcceptQuery() {
        mysqlChannel.suspendAcceptQuery();
    }

    public void resumeAcceptQuery() {
        mysqlChannel.resumeAcceptQuery();
    }

    public void stopAcceptQuery() throws IOException {
        mysqlChannel.stopAcceptQuery();
    }

    public String getQueryIdentifier() {
        return "stmt[" + stmtId + ", " + DebugUtil.printId(queryId) + "]";
    }

    public boolean supportHandleByFe() {
        return !getConnectType().equals(ConnectType.ARROW_FLIGHT_SQL) && getCommand() != MysqlCommand.COM_STMT_EXECUTE;
    }

    public void setCloudCluster(String cluster) {
        this.getSessionVariable().setCloudCluster(cluster);
    }

    public String getCloudCluster() throws ComputeGroupException {
        return getCloudCluster(true);
    }

    public static class CloudClusterResult {
        public enum Comment {
            FOUND_BY_DEFAULT_CLUSTER,
            DEFAULT_CLUSTER_SET_BUT_NOT_EXIST,
            FOUND_BY_FIRST_CLUSTER_WITH_ALIVE_BE,
            FOUND_BY_FRIST_CLUSTER_HAS_AUTH,
        }

        public String clusterName;
        public Comment comment;

        public CloudClusterResult(final String name, Comment c) {
            this.clusterName = name;
            this.comment = c;
        }

        @Override
        public String toString() {
            return "CloudClusterResult{"
                + "clusterName='" + clusterName + '\''
                + ", comment=" + comment
                + '}';
        }
    }

    // can't get cluster from context, use the following strategy to obtain the cluster name
    // 当用户有多个集群的权限时，会按照如下策略进行拉取：
    // 如果当前mysql用户没有指定cluster(没有default 或者 use), 选择有权限的cluster。
    // 如果有多个cluster满足权限条件,优先选活的，按字母序选
    // 如果没有活的，则拉起一个，按字母序选
    public CloudClusterResult getCloudClusterByPolicy() {
        List<String> cloudClusterNames = ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudClusterNames();
        // try set default cluster
        String defaultCloudCluster = Env.getCurrentEnv().getAuth().getDefaultCloudCluster(getQualifiedUser());
        if (!Strings.isNullOrEmpty(defaultCloudCluster)) {
            // check cluster validity
            CloudClusterResult r;
            if (cloudClusterNames.contains(defaultCloudCluster)) {
                // valid
                r = new CloudClusterResult(defaultCloudCluster,
                    CloudClusterResult.Comment.FOUND_BY_DEFAULT_CLUSTER);
                LOG.info("use default compute group {}", defaultCloudCluster);
            } else {
                // invalid
                r = new CloudClusterResult(defaultCloudCluster,
                    CloudClusterResult.Comment.DEFAULT_CLUSTER_SET_BUT_NOT_EXIST);
                LOG.warn("default compute group {} current invalid, please change it", r);
            }
            return r;
        }

        List<String> hasAuthCluster = new ArrayList<>();
        // get all available cluster of the user
        for (String cloudClusterName : cloudClusterNames) {
            if (Env.getCurrentEnv().getAccessManager().checkCloudPriv(getCurrentUserIdentity(),
                    cloudClusterName, PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER)) {
                hasAuthCluster.add(cloudClusterName);
                // find a cluster has more than one alive be
                List<Backend> bes = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                        .getBackendsByClusterName(cloudClusterName);
                AtomicBoolean hasAliveBe = new AtomicBoolean(false);
                bes.stream().filter(Backend::isAlive).findAny().ifPresent(backend -> {
                    LOG.debug("get a compute group {}, it's has more than one alive be {}", cloudCluster, backend);
                    hasAliveBe.set(true);
                });
                if (hasAliveBe.get()) {
                    // set a cluster to context cloudCluster
                    CloudClusterResult r = new CloudClusterResult(cloudClusterName,
                            CloudClusterResult.Comment.FOUND_BY_FIRST_CLUSTER_WITH_ALIVE_BE);
                    LOG.debug("set context {}", r);
                    return r;
                }
            }
        }
        return hasAuthCluster.isEmpty() ? null
            : new CloudClusterResult(hasAuthCluster.get(0), CloudClusterResult.Comment.FOUND_BY_FRIST_CLUSTER_HAS_AUTH);
    }

    public ComputeGroup getComputeGroupSafely() {
        try {
            return getComputeGroup();
        } catch (UserException e) {
            throw new RuntimeException(e);
        }
    }

    public ComputeGroup getComputeGroup() throws UserException {
        ComputeGroupMgr cgMgr = Env.getCurrentEnv().getComputeGroupMgr();
        if (Config.isCloudMode()) {
            return cgMgr.getComputeGroupByName(getCloudCluster());
        } else {
            // In order to be compatible with resource tag's old logic,
            // when a user login in FE by mysql client, then its tags are set in ConnectContext which
            // means isSetComputeGroup = true
            if (this.isSetComputeGroup()) {
                return computeGroup;
            } else {
                String currentUser = getQualifiedUser();
                if (!StringUtils.isEmpty(currentUser)) {
                    return Env.getCurrentEnv().getAuth().getComputeGroup(currentUser);
                } else {
                    return Env.getCurrentEnv().getComputeGroupMgr().getComputeGroupByName(Tag.VALUE_DEFAULT_TAG);
                }
            }
        }
    }

    /**
     * Tries to choose an available cluster in the following order
     * 1. Do nothing if a cluster has been chosen for current session. It may be
     *    chosen explicitly by `use @` command or setCloudCluster() or this method
     * 2. Tries to choose a default cluster if current mysql user has been set any
     * 3. Tries to choose an authorized cluster if all preceeding conditions failed
     *
     * @param updateErr whether set the connect state to error if the returned cluster is null or empty
     * @return non-empty cluster name if a cluster has been chosen otherwise null or empty string
     * @throws ComputeGroupException, outer get reason by exception
     */
    public String getCloudCluster(boolean updateErr) throws ComputeGroupException {
        if (!Config.isCloudMode()) {
            throw new ComputeGroupException("not cloud mode", ComputeGroupException.FailedTypeEnum.NOT_CLOUD_MODE);
        }

        String choseWay = null;
        // 1 get cluster from session
        String sessionCluster = getSessionVariable().getCloudCluster();
        if (!Strings.isNullOrEmpty(sessionCluster)) {
            choseWay = "use session";
            if (LOG.isDebugEnabled()) {
                LOG.debug("finally set context compute group name {} for user {} with chose way '{}'",
                        sessionCluster, getCurrentUserIdentity(), choseWay);
            }
            return sessionCluster;
        }

        // 2 get cluster from user
        String userPropCluster = getDefaultCloudClusterFromUser();
        if (!StringUtils.isEmpty(userPropCluster)) {
            choseWay = "user property";
            if (LOG.isDebugEnabled()) {
                LOG.debug("finally set context compute group name {} for user {} with chose way '{}'", userPropCluster,
                        getCurrentUserIdentity(), choseWay);
            }
            return userPropCluster;
        }

        // 3 get cluster from a cached variable in connect context
        // this value comes from a cluster selection policy
        if (!Strings.isNullOrEmpty(this.cloudCluster)) {
            choseWay = "user selection policy";
            if (LOG.isDebugEnabled()) {
                LOG.debug("finally set context compute group name {} for user {} with chose way '{}'",
                        cloudCluster, getCurrentUserIdentity(), choseWay);
            }
            return cloudCluster;
        }

        String policyCluster = "";
        CloudClusterResult cloudClusterTypeAndName = getCloudClusterByPolicy();
        if (cloudClusterTypeAndName != null && !Strings.isNullOrEmpty(cloudClusterTypeAndName.clusterName)) {
            policyCluster = cloudClusterTypeAndName.clusterName;
            choseWay = "by policy";
        }

        if (Strings.isNullOrEmpty(policyCluster)) {
            List<String> cloudClusterNames
                    = ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudClusterNames();
            LOG.warn("Can not get a valid compute group for user {} {} to use, all cluster: {}",
                    getCurrentUserIdentity(),
                    getQualifiedUser(), cloudClusterNames);
            ComputeGroupException exception = new ComputeGroupException(
                    "the user is not granted permission to the compute group",
                    ComputeGroupException.FailedTypeEnum.CURRENT_USER_NO_AUTH_TO_USE_ANY_COMPUTE_GROUP);
            if (updateErr) {
                getState().setError(ErrorCode.ERR_CLOUD_CLUSTER_ERROR, exception.getMessage());
            }
            throw exception;
        } else {
            this.cloudCluster = policyCluster;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("finally set context compute group name {} for user {} with chose way '{}'", this.cloudCluster,
                    getCurrentUserIdentity(), choseWay);
        }

        return this.cloudCluster;
    }

    // TODO implement this function
    private String getDefaultCloudClusterFromUser() {
        List<String> cloudClusterNames = ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudClusterNames();
        String defaultCluster = Env.getCurrentEnv().getAuth().getDefaultCloudCluster(getQualifiedUser());
        if (!Strings.isNullOrEmpty(defaultCluster) && cloudClusterNames.contains(defaultCluster)) {
            return defaultCluster;
        }

        return null;
    }

    public StatsErrorEstimator getStatsErrorEstimator() {
        return statsErrorEstimator;
    }

    public void setStatsErrorEstimator(StatsErrorEstimator statsErrorEstimator) {
        this.statsErrorEstimator = statsErrorEstimator;
    }

    public void setWorkloadGroupName(String workloadGroupName) {
        this.workloadGroupName = workloadGroupName;
    }

    public String getWorkloadGroupName() {
        return this.workloadGroupName;
    }

    public boolean isSkipAuth() {
        return skipAuth;
    }

    public void setSkipAuth(boolean skipAuth) {
        this.skipAuth = skipAuth;
    }

    public boolean isRunProcedure() {
        return runProcedure;
    }

    public void setRunProcedure(boolean runProcedure) {
        this.runProcedure = runProcedure;
    }

    public void setProcedureExec(Exec exec) {
        this.exec = exec;
    }

    public Exec getProcedureExec() {
        return exec;
    }

    public int getNetReadTimeout() {
        return this.sessionVariable.getNetReadTimeout();
    }

    public int getNetWriteTimeout() {
        return this.sessionVariable.getNetWriteTimeout();
    }

    public boolean isGroupCommit() {
        return isGroupCommit;
    }

    public void setGroupCommit(boolean groupCommit) {
        isGroupCommit = groupCommit;
    }

    public Map<String, LiteralExpr> getUserVars() {
        return userVars;
    }

    public void setUserVars(Map<String, LiteralExpr> userVars) {
        this.userVars = userVars;
    }

    public boolean isProxy() {
        return isProxy;
    }

    public void setMysqlHandshakePacket(MysqlHandshakePacket mysqlHandshakePacket) {
        this.mysqlHandshakePacket = mysqlHandshakePacket;
    }

    public byte[] getAuthPluginData() {
        return mysqlHandshakePacket == null ? null : mysqlHandshakePacket.getAuthPluginData();
    }

    @Override
    public String toString() {
        return getClass().getName() + "@" + Integer.toHexString(hashCode()) + ":" + getQualifiedUser();
    }

    public Map<String, Set<String>> getDbToTempTableNamesMap() {
        return dbToTempTableNamesMap;
    }

    public void addTempTableToDB(String database, String tableName) {
        Set<String> tableNameSet = dbToTempTableNamesMap.get(database);
        if (tableNameSet == null) {
            tableNameSet = new HashSet<>();
            dbToTempTableNamesMap.put(database, tableNameSet);
        }

        tableNameSet.add(tableName);
    }

    public void removeTempTableFromDB(String database, String tableName) {
        Set<String> tableNameSet = dbToTempTableNamesMap.get(database);
        if (tableNameSet != null) {
            tableNameSet.remove(tableName);
        }
    }
}
