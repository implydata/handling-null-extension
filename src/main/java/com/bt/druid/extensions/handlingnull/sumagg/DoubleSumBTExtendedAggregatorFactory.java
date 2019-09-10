package com.bt.druid.extensions.handlingnull.sumagg;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.*;
import org.apache.druid.segment.ColumnSelectorFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class DoubleSumBTExtendedAggregatorFactory extends AggregatorFactory {
    public static final String TYPE_NAME = "doubleSumExtended";
    private final String name;
    private final String fieldName;

    @JsonCreator
    public DoubleSumBTExtendedAggregatorFactory(@JsonProperty("name") String name,
                                                @JsonProperty("fieldName") final String fieldName) {
        Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
        Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");
        this.name = name;
        this.fieldName = fieldName;
    }

    @Override
    public Aggregator factorize(ColumnSelectorFactory columnSelectorFactory) {
        return new DoubleSumBTExtendedAggregator(columnSelectorFactory.makeColumnValueSelector(fieldName));
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnSelectorFactory) {
        return new DoubleSumBufferBTExtendedAggregator(columnSelectorFactory.makeColumnValueSelector(fieldName));
    }

    @Override
    public Comparator getComparator() {
        return DoubleSumBTExtendedAggregator.COMPARATOR;
    }

    @Nullable
    @Override
    public Object combine(@Nullable Object lhs, @Nullable Object rhs) {
        if (rhs == null) {
            return lhs;
        }
        if (lhs == null) {
            return rhs;
        }
        return DoubleSumBTExtendedAggregator.combineValues(lhs, rhs);
    }

    @Override
    public AggregatorFactory getCombiningFactory() {
        return new DoubleSumBTExtendedAggregatorFactory(name, name);
    }

    @Override
    public AggregateCombiner makeAggregateCombiner() {
        return new DoubleSumBTExtendedAggregateCombiner();
    }

    @Override
    public List<AggregatorFactory> getRequiredColumns() {
        return Collections.singletonList(new DoubleSumBTExtendedAggregatorFactory(fieldName, fieldName));
    }

    @Override
    public Object deserialize(Object object) {
        if (object instanceof String) {
            return Double.parseDouble((String) object);
        }
        return object;
    }

    @Nullable
    @Override
    public Object finalizeComputation(@Nullable Object object) {
        return object;
    }

    @Override
    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public List<String> requiredFields() {
        return Collections.singletonList(fieldName);
    }

    @Override
    public String getTypeName() {
        return "double";
    }

    @Override
    public int getMaxIntermediateSize() {
        return Double.BYTES;
    }

    @Override
    public byte[] getCacheKey() {
        byte[] fieldNameBytes = StringUtils.toUtf8WithNullToEmpty(fieldName);
        byte[] nameBytes = StringUtils.toUtf8WithNullToEmpty(name);

        return ByteBuffer.allocate(3 + fieldNameBytes.length + nameBytes.length)
                .put(AggregatorUtil.DOUBLE_SUM_CACHE_TYPE_ID)
                .put(fieldNameBytes)
                .put(AggregatorUtil.STRING_SEPARATOR)
                .put(nameBytes)
                .array();
    }
}
