package com.sj.flink.stream.operators;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class IncrementMapTest {

    @Test
    public void testIncrement() throws Exception {
        // instantiate your function
        IncrementMap incrementer = new IncrementMap();

        // call the methods that you have implemented
        assertEquals(Long.valueOf(3L), incrementer.map(Long.valueOf(2L)));

    }
}