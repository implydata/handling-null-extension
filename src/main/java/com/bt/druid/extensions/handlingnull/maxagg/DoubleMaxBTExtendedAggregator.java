package com.bt.druid.extensions.handlingnull.maxagg;

import com.google.common.collect.Ordering;
import com.google.common.primitives.Doubles;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;

import java.util.Comparator;

public class DoubleMaxBTExtendedAggregator implements Aggregator {

    static final Comparator COMPARATOR = new Ordering()
    {
        @Override
        public int compare(Object o1, Object o2)
        {
            if( ((Number) o1).doubleValue() == Integer.MIN_VALUE )
                return -1;

            if( ((Number) o2).doubleValue() == Integer.MIN_VALUE )
                return 1;

            return Doubles.compare(((Number) o1).doubleValue(), ((Number) o2).doubleValue());
        }
    }.nullsFirst();

    static double combineValues(Object lhs, Object rhs)
    {
        double lhsDouble = ((Number) lhs).doubleValue();
        double rhsDouble = ((Number) rhs).doubleValue();


        if(lhsDouble == Integer.MIN_VALUE) {
            return rhsDouble;
        } else if(rhsDouble == Integer.MIN_VALUE) {
            return lhsDouble;
        }

        return Math.max(lhsDouble, rhsDouble);
    }

    private final BaseDoubleColumnValueSelector selector;

    private double max;

    public DoubleMaxBTExtendedAggregator(BaseDoubleColumnValueSelector selector)
    {
        this.selector = selector;
        this.max = Double.NEGATIVE_INFINITY;
    }

    @Override
    public void aggregate()
    {
        if(selector.getDouble() != Integer.MIN_VALUE) {
            max = Math.max(max, selector.getDouble());
        }
    }

    @Override
    public Object get() {
        return getValue();
    }

    @Override
    public float getFloat() {
        return (float) getValue();
    }

    @Override
    public long getLong() {
        return (long) getValue();
    }

    @Override
    public double getDouble() {
        return (double) getValue();
    }

    private Object getValue() {
        if(max == Double.NEGATIVE_INFINITY)
            return (double)Integer.MIN_VALUE;

        return max;
    }

    @Override
    public void close()
    {
        // no resources to cleanup
    }

}
