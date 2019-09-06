package com.bt.druid.extensions.handlingnull;

import org.apache.druid.segment.BaseDoubleColumnValueSelector;

import java.nio.ByteBuffer;

public class DoubleMinBufferBTExtendedAggregator extends SimpleDoubleBufferBTExtendedAggregator {

    DoubleMinBufferBTExtendedAggregator(BaseDoubleColumnValueSelector selector)
    {
        super(selector);
    }

    @Override
    public void init(ByteBuffer buf, int position)
    {
        buf.putDouble(position, Double.POSITIVE_INFINITY);
    }

    @Override
    public void putFirst(ByteBuffer buf, int position, double value)
    {
        if (!Double.isNaN(value) && value != Integer.MIN_VALUE) {
            buf.putDouble(position, value);
        } else {
            init(buf, position);
        }
    }

    @Override
    public void aggregate(ByteBuffer buf, int position, double value)
    {
        if(value != Integer.MIN_VALUE) {
            buf.putDouble(position, Math.min(buf.getDouble(position), value));
        }
    }
}
