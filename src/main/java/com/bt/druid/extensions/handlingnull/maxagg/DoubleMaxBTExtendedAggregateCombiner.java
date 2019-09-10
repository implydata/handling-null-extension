package com.bt.druid.extensions.handlingnull.maxagg;

import org.apache.druid.query.aggregation.DoubleAggregateCombiner;
import org.apache.druid.segment.ColumnValueSelector;

public class DoubleMaxBTExtendedAggregateCombiner extends DoubleAggregateCombiner {

    private double max = Double.NEGATIVE_INFINITY;

    @Override
    public void reset(ColumnValueSelector selector)
    {
        if(selector.getDouble() != Integer.MIN_VALUE) {
            max = selector.getDouble();
        }
    }

    @Override
    public void fold(ColumnValueSelector selector)
    {
        if(selector.getDouble() != Integer.MIN_VALUE) {
            max = Math.max(max, selector.getDouble());
        }
    }

    @Override
    public double getDouble() {
        if(max == Double.NEGATIVE_INFINITY)
            return (double)Integer.MIN_VALUE;

        return max;
    }

}
