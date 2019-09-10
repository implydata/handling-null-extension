package com.bt.druid.extensions.handlingnull.sumagg;

import org.apache.druid.query.aggregation.DoubleAggregateCombiner;
import org.apache.druid.segment.ColumnValueSelector;

public class DoubleSumBTExtendedAggregateCombiner extends DoubleAggregateCombiner {
    private double sum = Double.POSITIVE_INFINITY;

    @Override
    public void reset(ColumnValueSelector selector) {
        if(selector.getDouble() != Integer.MIN_VALUE) {
            sum = selector.getDouble();
        }
    }

    @Override
    public void fold(ColumnValueSelector selector) {
        if(selector.getDouble() != Integer.MIN_VALUE) {
            if(this.sum == Double.POSITIVE_INFINITY)
                this.sum = selector.getDouble();
            else
                sum += selector.getDouble();
        }
    }

    @Override
    public double getDouble() {
        if(sum == Double.POSITIVE_INFINITY)
            return (double)Integer.MIN_VALUE;

        return sum;
    }

}
