package org.easyjob.allocation.table;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class NodeCapacityInfo {

    private String id;
    private int maxCapacity;
    private int usedCapacity;

    public int getFreeCapacity() {
        return maxCapacity - usedCapacity;
    }

    public double getUsageRate() {
        return usedCapacity * 1d / maxCapacity;
    }

    public double getUsageRateStd(double avgRate) {
        return this.getUsageRate() - avgRate;
    }


}