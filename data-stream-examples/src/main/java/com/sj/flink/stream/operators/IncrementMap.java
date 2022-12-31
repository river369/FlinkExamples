package com.sj.flink.stream.operators;

import org.apache.flink.api.common.functions.MapFunction;

public class IncrementMap implements MapFunction<Long, Long> {

    @Override
    public Long map(Long record) throws Exception {
        return record + 1;
    }
}