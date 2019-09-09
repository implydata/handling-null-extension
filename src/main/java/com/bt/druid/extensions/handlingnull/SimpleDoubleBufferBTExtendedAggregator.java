package com.bt.druid.extensions.handlingnull;

import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.CalledFromHotLoop;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;

import java.nio.ByteBuffer;

public abstract class SimpleDoubleBufferBTExtendedAggregator implements BufferAggregator {
    final BaseDoubleColumnValueSelector selector;

    SimpleDoubleBufferBTExtendedAggregator(BaseDoubleColumnValueSelector selector)
    {
        this.selector = selector;
    }

    public BaseDoubleColumnValueSelector getSelector()
    {
        return selector;
    }

    /**
     * Faster equivalent to
     * aggregator.init(buf, position);
     * aggregator.aggregate(buf, position, value);
     */
    @CalledFromHotLoop
    public abstract void putFirst(ByteBuffer buf, int position, double value);

    @CalledFromHotLoop
    public abstract void aggregate(ByteBuffer buf, int position, double value);

    @Override
    public final void aggregate(ByteBuffer buf, int position)
    {
        aggregate(buf, position, selector.getDouble());
    }

    @Override
    public final Object get(ByteBuffer buf, int position)
    {
        return getValue(buf, position);
    }

    @Override
    public final float getFloat(ByteBuffer buf, int position)
    {
        return (float) getValue(buf, position);
    }

    @Override
    public final long getLong(ByteBuffer buf, int position)
    {
        return (long) getValue(buf, position);
    }

    @Override
    public double getDouble(ByteBuffer buffer, int position)
    {
        return (double) getValue(buffer, position);
    }

    private Object getValue(ByteBuffer buf, int position) {
        double value = buf.getDouble(position);

        if(value == Double.POSITIVE_INFINITY)
            return (double)Integer.MIN_VALUE;

        return value;
    }

    @Override
    public void close()
    {
        // no resources to cleanup
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
        inspector.visit("selector", selector);
    }
}
