package com.bt.druid.extensions;

import com.bt.druid.extensions.handlingnull.minagg.DoubleMinBTExtendedAggregatorFactory;
import com.bt.druid.extensions.handlingnull.maxagg.DoubleMaxBTExtendedAggregatorFactory;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import org.apache.druid.initialization.DruidModule;

import java.util.List;

public class CustomModules implements DruidModule {
    @Override
    public List<? extends com.fasterxml.jackson.databind.Module> getJacksonModules() {

        return ImmutableList.of(
                new SimpleModule(
                        getClass().getSimpleName()
                ).registerSubtypes(
                        new NamedType(
                                DoubleMinBTExtendedAggregatorFactory.class,
                                DoubleMinBTExtendedAggregatorFactory.TYPE_NAME
                        ),
                        new NamedType(
                                DoubleMaxBTExtendedAggregatorFactory.class,
                                DoubleMaxBTExtendedAggregatorFactory.TYPE_NAME
                        )
                )
        );
    }

    @Override
    public void configure(Binder binder) {

    }
}
