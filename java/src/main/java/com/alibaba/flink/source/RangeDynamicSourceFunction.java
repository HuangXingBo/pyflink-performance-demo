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

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;

public class RangeDynamicSourceFunction implements SourceFunction<RowData> {

    private static final long serialVersionUID = 1L;

    private boolean stopped = false;

    private final int start;
    private final int end;
    private final int step;
    private final int partition;

    public RangeDynamicSourceFunction(int start, int end, int step, int partition) {
        this.start = start;
        this.end = end;
        this.step = step;
        this.partition = partition;
    }

    @Override
    public void run(SourceContext<RowData> sourceContext) throws Exception {
        int perPartitionNum = ((end - start) / step + 1) / partition;
        int num = 0;
        int curPartition = 0;
        for (int id = start; id <= end && !stopped; id += step) {
            GenericRowData reuseRow = new GenericRowData(3);
            reuseRow.setField(0, id);
            reuseRow.setField(1, num);
            reuseRow.setField(2, TimestampData.fromEpochMillis(curPartition));
            num++;
            if (num == perPartitionNum) {
                num = 0;
                curPartition++;
            }
            sourceContext.collect(reuseRow);
        }
    }

    @Override
    public void cancel() {
        stopped = true;
    }
}
