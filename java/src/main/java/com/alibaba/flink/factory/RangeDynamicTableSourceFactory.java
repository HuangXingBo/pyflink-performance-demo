/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.factory;

import com.alibaba.flink.source.RangeDynamicTableSource;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;

public class RangeDynamicTableSourceFactory implements DynamicTableSourceFactory {

    public static final ConfigOption<Integer> START = key("start")
            .intType()
            .defaultValue(0)
            .withDescription("start");

    public static final ConfigOption<Integer> END = key("end")
            .intType()
            .defaultValue(10000)
            .withDescription("end");

    public static final ConfigOption<Integer> STEP = key("step")
            .intType()
            .defaultValue(1)
            .withDescription("step");

    public static final ConfigOption<Integer> PARTITION = key("partition")
            .intType()
            .defaultValue(1)
            .withDescription("partition");

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        Configuration options = new Configuration();
        context.getCatalogTable().getOptions().forEach(options::setString);
        return new RangeDynamicTableSource(options.get(START), options.get(END), options.get(STEP), options.get(PARTITION));
    }

    @Override
    public String factoryIdentifier() {
        return "Range";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(START);
        options.add(END);
        options.add(STEP);
        options.add(PARTITION);
        return options;
    }
}
