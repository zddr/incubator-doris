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

import org.apache.doris.mtmv.MTMVJobInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.mtmv.MTMVRefreshInfo;
import org.apache.doris.mtmv.MTMVRefreshSnapshot;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVStatus;

import com.google.gson.annotations.SerializedName;

import java.util.Map;


public class MTMVCopy{
    @SerializedName("refreshInfo")
    private MTMVRefreshInfo refreshInfo;
    @SerializedName("querySql")
    private String querySql;
    @SerializedName("status")
    private MTMVStatus status;
    @SerializedName("jobInfo")
    private MTMVJobInfo jobInfo;
    @SerializedName("mvProperties")
    private Map<String, String> mvProperties;
    @SerializedName("relation")
    private MTMVRelation relation;
    @SerializedName("mvPartitionInfo")
    private MTMVPartitionInfo mvPartitionInfo;
    @SerializedName("refreshSnapshot")
    private MTMVRefreshSnapshot refreshSnapshot;

    public MTMVCopy(MTMV mtmv) {
        this.refreshInfo=mtmv.getRefreshInfo();
        this.querySql=mtmv.getQuerySql();
        this.status=mtmv.getStatus();
        this.jobInfo=mtmv.getJobInfo();
        this.mvProperties=mtmv.getMvProperties();
        this.relation=mtmv.getRelation();
        this.mvPartitionInfo=mtmv.getMvPartitionInfo();
        this.refreshSnapshot=mtmv.getRefreshSnapshot();

    }
}
