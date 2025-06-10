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

package org.apache.doris.catalog;

import org.apache.doris.analysis.MVRefreshInfo;
import org.apache.doris.analysis.MVRefreshInfo.BuildMode;
import org.apache.doris.analysis.MVRefreshInfo.RefreshTrigger;
import org.apache.doris.catalog.OlapTableFactory.MaterializedViewParams;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.Text;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.mtmv.MTMVJobFactory;
import org.apache.doris.mtmv.metadata.MTMVJob;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;


public class MaterializedView extends OlapTable {
    private static final Logger LOG = LogManager.getLogger(MaterializedView.class);
    @SerializedName("buildMode")
    private BuildMode buildMode;
    @SerializedName("refreshInfo")
    private MVRefreshInfo refreshInfo;
    @SerializedName("query")
    private String query;
    @SerializedName("baseTables")
    private Set<String> baseTables;

    private final ReentrantLock mvTaskLock = new ReentrantLock(true);

    public boolean tryLockMVTask() {
        try {
            return mvTaskLock.tryLock(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    public void unLockMVTask() {
        this.mvTaskLock.unlock();
    }

    // For deserialization
    public MaterializedView() {
        type = TableType.MATERIALIZED_VIEW;
    }

    MaterializedView(MaterializedViewParams params) {
        super(
                params.tableId,
                params.tableName,
                params.schema,
                params.keysType,
                params.partitionInfo,
                params.distributionInfo
        );
        type = TableType.MATERIALIZED_VIEW;
        buildMode = params.buildMode;
        refreshInfo = params.mvRefreshInfo;
        query = params.queryStmt.toSqlWithHint();
        baseTables = params.baseTables;
    }

    public BuildMode getBuildMode() {
        return buildMode;
    }

    public MVRefreshInfo getRefreshInfo() {
        return refreshInfo;
    }

    public  void setRefreshInfo(MVRefreshInfo info) {
        refreshInfo = info;
    }

    public String getQuery() {
        return query;
    }

    public Set<String> getBaseTables() {
        return baseTables;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        MaterializedView materializedView = GsonUtils.GSON.fromJson(Text.readString(in), this.getClass());
        refreshInfo = materializedView.refreshInfo;
        query = materializedView.query;
        buildMode = materializedView.buildMode;
    }

    public MaterializedView clone(String mvName) throws IOException {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeConstants.meta_version);
        metaContext.setThreadLocalInfo();
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream(256);
            MaterializedView cloned = new MaterializedView();
            this.write(new DataOutputStream(out));
            cloned.readFields(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
            cloned.setName(mvName);
            return cloned;
        } finally {
            MetaContext.remove();
        }
    }

    public void onCommit(TableIf tableIf) {
        if (!refreshInfo.getTriggerInfo().getRefreshTrigger().equals(RefreshTrigger.COMMIT)) {
            return;
        }
        if (!baseTables.contains(tableIf.getName())) {
            return;
        }
        MTMVJob mtmvJob = MTMVJobFactory.genOnceJob(this, this.getQualifiedDbName());
        try {
            Env.getCurrentEnv().getMTMVJobManager().createJob(mtmvJob, false);
        } catch (DdlException e) {
            LOG.warn("onCommit failed,mvName:{},tableName:{}", getName(), tableIf.getName(), e);
        }
    }
}
