package com.bt.druid.extensions.handlingnull.sumagg;

import com.bt.druid.extensions.handlingnull.SimpleDoubleBufferBTExtendedAggregator;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;

import java.nio.ByteBuffer;

public class DoubleSumBufferBTExtendedAggregator extends SimpleDoubleBufferBTExtendedAggregator {

    DoubleSumBufferBTExtendedAggregator(BaseDoubleColumnValueSelector selector) {
        super(selector);
    }

    @Override
    public void init(ByteBuffer buf, int position) {
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
    public void aggregate(ByteBuffer buf, int position, double value) {
        if(value != Integer.MIN_VALUE) {
            double currentValue = buf.getDouble(position);
            if(currentValue == Double.POSITIVE_INFINITY)
                buf.putDouble(position, value);
            else
                buf.putDouble(position, currentValue + value);
        }
    }

}
