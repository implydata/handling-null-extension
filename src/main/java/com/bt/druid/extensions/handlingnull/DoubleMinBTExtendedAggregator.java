package com.bt.druid.extensions.handlingnull;

import com.google.common.collect.Ordering;
import com.google.common.primitives.Doubles;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;

import java.util.Comparator;

public class DoubleMinBTExtendedAggregator implements Aggregator {

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

        return Math.min(lhsDouble, rhsDouble);
    }

    private final BaseDoubleColumnValueSelector selector;

    private double min;

    public DoubleMinBTExtendedAggregator(BaseDoubleColumnValueSelector selector)
    {
        this.selector = selector;
        this.min = Double.POSITIVE_INFINITY;
    }

    @Override
    public void aggregate()
    {
        if(selector.getDouble() != Integer.MIN_VALUE) {
            min = Math.min(min, selector.getDouble());
        }
    }

    @Override
    public Object get()
    {
        return min;
    }

    @Override
    public float getFloat()
    {
        return (float) min;
    }

    @Override
    public long getLong()
    {
        return (long) min;
    }

    @Override
    public double getDouble()
    {
        return min;
    }

    @Override
    public void close()
    {
        // no resources to cleanup
    }

}
