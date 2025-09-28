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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.mvcc.MvccUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * get all related partition descs
 */
public class MTMVRelatedPartitionDescInitGenerator implements MTMVRelatedPartitionDescGeneratorService {

    @Override
    public void apply(MTMVPartitionInfo mvPartitionInfo, Map<String, String> mvProperties,
            RelatedPartitionDescResult lastResult) throws AnalysisException {
        List<MTMVRelatedTableIf> relatedTables = Lists.newArrayList();
        Map<PartitionItem, PartitionIdents> res = Maps.newHashMap();
        for (MTMVRelatedTableIf relatedTable : relatedTables) {
            Map<PartitionItem, PartitionIdent> partitionItems = getRelatedTablePartitionItems(relatedTable);
            for (Entry<PartitionItem, PartitionIdent> entry : partitionItems.entrySet()) {
                PartitionIdents partitionIdents = res.computeIfAbsent(entry.getKey(), k -> new PartitionIdents());
                partitionIdents.addPartitionIdent(entry.getValue());
            }

        }
        // todo: 调用getIntersect检查res的key可以是否有交集
        lastResult.setItems(res);
    }

    private Map<PartitionItem, PartitionIdent> getRelatedTablePartitionItems(MTMVRelatedTableIf relatedTable)
            throws AnalysisException {
        Map<String, PartitionItem> partitionItems = relatedTable.getAndCopyPartitionItems(
                MvccUtil.getSnapshotFromContext(relatedTable));
        BaseTableInfo baseTableInfo = new BaseTableInfo(relatedTable);
        Map<PartitionItem, PartitionIdent> res = Maps.newHashMap();
        for (Entry<String, PartitionItem> entry : partitionItems.entrySet()) {
            res.put(entry.getValue(), new PartitionIdent(baseTableInfo, entry.getKey()));
        }
        return res;
    }
}
