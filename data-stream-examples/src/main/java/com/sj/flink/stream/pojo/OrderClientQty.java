package com.sj.flink.stream.pojo;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class OrderClientQty {
    String client;
    Integer quantity;
}
