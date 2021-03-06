package com.bt.druid.extensions.handlingnull.minagg;

import org.apache.druid.query.aggregation.DoubleAggregateCombiner;
import org.apache.druid.segment.ColumnValueSelector;

final class DoubleMinBTExtendedAggregateCombiner extends DoubleAggregateCombiner {
    private double min  = Double.POSITIVE_INFINITY;

    @Override
    public void reset(ColumnValueSelector selector)
    {
        if(selector.getDouble() != Integer.MIN_VALUE) {
            min = selector.getDouble();
        }
    }

    @Override
    public void fold(ColumnValueSelector selector)
    {
        if(selector.getDouble() != Integer.MIN_VALUE) {
            min = Math.min(min, selector.getDouble());
        }
    }

    @Override
    public double getDouble() {
        if(min == Double.POSITIVE_INFINITY)
            return (double)Integer.MIN_VALUE;

        return min;
    }
}
