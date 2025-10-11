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

import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.RangeUtils;

import com.google.common.collect.Range;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * get all related partition descs
 */
public class MTMVRelatedPartitionDescGatherGenerator implements MTMVRelatedPartitionDescGeneratorService {

    @Override
    public void apply(MTMVPartitionInfo mvPartitionInfo, Map<String, String> mvProperties,
            RelatedPartitionDescResult lastResult) throws AnalysisException {
        Map<MTMVRelatedTableIf, Map<PartitionKeyDesc, Set<String>>> descs = lastResult.getDescs();
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> res = new HashMap<>();
        for (Entry<MTMVRelatedTableIf, Map<PartitionKeyDesc, Set<String>>> entry : descs.entrySet()) {
            MTMVRelatedTableIf relatedTable = entry.getKey();
            Map<PartitionKeyDesc, Set<String>> relatedDescs = entry.getValue();
            for (Entry<PartitionKeyDesc, Set<String>> entry1 : relatedDescs.entrySet()) {
                PartitionKeyDesc partitionKeyDesc = entry1.getKey();
                Set<String> partitionNames = entry1.getValue();
                Map<MTMVRelatedTableIf, Set<String>> partitionKeyDescMap = res.computeIfAbsent(partitionKeyDesc,
                        k -> new HashMap<>());
                partitionKeyDescMap.put(relatedTable, partitionNames);
            }
        }
        // 如果有多个relatedTable，校验所有的PartitionKeyDesc是否有交叉
        mvPartitionInfo.
        for (PartitionKeyDesc partitionKeyDesc : res.keySet()) {

        }
        lastResult.setRes(res);
    }

    private Range<PartitionKey> createAndCheckNewRange(PartitionKeyDesc partKeyDesc, boolean isTemp)
            throws AnalysisException, DdlException {
        boolean isFixedPartitionKeyValueType
                = partKeyDesc.getPartitionType() == PartitionKeyDesc.PartitionKeyValueType.FIXED;

        // generate partitionItemEntryList
        List<Entry<Long, PartitionItem>> partitionItemEntryList;

        if (isFixedPartitionKeyValueType) {
            return createNewRangeForFixedPartitionValueType(partKeyDesc);
        } else {
            Range<PartitionKey> newRange = null;
            // create upper values for new range
            PartitionKey newRangeUpper = null;
            if (partKeyDesc.isMax()) {
                newRangeUpper = PartitionKey.createInfinityPartitionKey(partitionColumns, true);
            } else {
                newRangeUpper = PartitionKey.createPartitionKey(partKeyDesc.getUpperValues(), partitionColumns);
            }
            if (newRangeUpper.isMinValue()) {
                throw new DdlException("Partition's upper value should not be MIN VALUE: " + partKeyDesc.toSql());
            }

            Range<PartitionKey> lastRange = null;
            Range<PartitionKey> currentRange = null;
            for (Map.Entry<Long, PartitionItem> entry : partitionItemEntryList) {
                currentRange = entry.getValue().getItems();
                // check if equals to upper bound
                PartitionKey upperKey = currentRange.upperEndpoint();
                if (upperKey.compareTo(newRangeUpper) >= 0) {
                    newRange = createNewRangeForLessThanPartitionValueType(newRangeUpper, lastRange, currentRange);
                    break;
                } else {
                    lastRange = currentRange;
                }
            } // end for ranges

            if (newRange == null) /* the new range's upper value is larger than any existing ranges */ {
                newRange = createNewRangeForLessThanPartitionValueType(newRangeUpper, lastRange, currentRange);
            }
            return newRange;
        }
    }

    private Range<PartitionKey> createNewRangeForFixedPartitionValueType(PartitionKeyDesc partKeyDesc)
            throws AnalysisException, DdlException {
        PartitionKey lowKey = PartitionKey.createPartitionKey(partKeyDesc.getLowerValues(), partitionColumns);
        PartitionKey upperKey =  PartitionKey.createPartitionKey(partKeyDesc.getUpperValues(), partitionColumns);
        if (lowKey.compareTo(upperKey) >= 0) {
            throw new AnalysisException("The lower values must smaller than upper values");
        }
        Range<PartitionKey> newRange = Range.closedOpen(lowKey, upperKey);
        for (Map.Entry<Long, PartitionItem> partitionItemEntry : partitionItemEntryList) {
            RangeUtils.checkRangeIntersect(newRange, partitionItemEntry.getValue().getItems());
        }
        return newRange;
    }
}
