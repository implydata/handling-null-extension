package com.bt.druid.extensions.handlingnull;

import org.apache.druid.query.aggregation.DoubleAggregateCombiner;
import org.apache.druid.segment.ColumnValueSelector;

final class DoubleMinBTExtendedAggregateCombiner extends DoubleAggregateCombiner {
    private double min;

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
    public double getDouble()
    {
        return min;
    }
}
