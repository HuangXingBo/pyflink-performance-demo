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
package com.alibaba.flink.source;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;

public class RangeDynamicTableSource implements ScanTableSource {

    private final int start;
    private final int end;
    private final int step;
    private final int partition;

    public RangeDynamicTableSource(int start, int end, int step, int partition) {
        this.start = start;
        this.end = end;
        this.step = step;
        this.partition = partition;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        RangeDynamicSourceFunction source = new RangeDynamicSourceFunction(start, end, step, partition);
        return SourceFunctionProvider.of(source, true);
    }

    @Override
    public DynamicTableSource copy() {
        return new RangeDynamicTableSource(start, end, step, partition);
    }

    @Override
    public String asSummaryString() {
        return "RangeDynamicTableSource";
    }
}
