package org.apache.doris.mtmv;

import com.google.gson.annotations.SerializedName;

public class MTMVPartitionRelatedInfo {
    @SerializedName("rt")
    private BaseTableInfo relatedTable;
    @SerializedName("rc")
    private String relatedCol;
}
