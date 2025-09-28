package org.apache.doris.mtmv;

public class PartitionIdent {
    private BaseTableInfo baseTableInfo;
    private String partitionName;

    public PartitionIdent(BaseTableInfo baseTableInfo, String partitionName) {
        this.partitionName = partitionName;
        this.baseTableInfo = baseTableInfo;
    }
}
