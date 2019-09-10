package com.bt.druid.extensions.handlingnull.maxagg;

import com.bt.druid.extensions.handlingnull.SimpleDoubleBufferBTExtendedAggregator;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;

import java.nio.ByteBuffer;

public class DoubleMaxBufferBTExtendedAggregator extends SimpleDoubleBufferBTExtendedAggregator {

    DoubleMaxBufferBTExtendedAggregator(BaseDoubleColumnValueSelector selector)
    {
        super(selector);
    }

    @Override
    public void init(ByteBuffer buf, int position)
    {
        buf.putDouble(position, Double.NEGATIVE_INFINITY);
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
            buf.putDouble(position, Math.max(buf.getDouble(position), value));
        }
    }

}
