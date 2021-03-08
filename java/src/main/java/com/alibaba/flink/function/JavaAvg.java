package com.alibaba.flink.function;

import org.apache.flink.table.functions.AggregateFunction;

public class JavaAvg extends AggregateFunction<Float, WeightedAvgAccumulator> {

    @Override
    public Float getValue(WeightedAvgAccumulator acc) {
        if (acc.count == 0) {
            return null;
        } else {
            return (acc.sum + 0.0F) / acc.count;
        }
    }

    public void accumulate(WeightedAvgAccumulator acc, Integer iValue) {
        acc.sum += iValue;
        acc.count += 1;
    }

    public void retract(WeightedAvgAccumulator acc, Integer iValue) {
        acc.sum -= iValue;
        acc.count -= 1;
    }

    public void merge(WeightedAvgAccumulator acc, Iterable<WeightedAvgAccumulator> it) {
        for (WeightedAvgAccumulator a : it) {
            acc.count += a.count;
            acc.sum += a.sum;
        }
    }

    @Override
    public WeightedAvgAccumulator createAccumulator() {
        return new WeightedAvgAccumulator();
    }
}
