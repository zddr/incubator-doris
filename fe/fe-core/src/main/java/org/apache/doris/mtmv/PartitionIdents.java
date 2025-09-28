package org.apache.doris.mtmv;

import java.util.ArrayList;
import java.util.List;

public class PartitionIdents {
    private List<PartitionIdent> partitionIdents = new ArrayList<>();

    public List<PartitionIdent> getPartitionIdents() {
        return partitionIdents;
    }

    public void addPartitionIdent(PartitionIdent partitionIdent) {
        this.partitionIdents.add(partitionIdent);
    }
}
