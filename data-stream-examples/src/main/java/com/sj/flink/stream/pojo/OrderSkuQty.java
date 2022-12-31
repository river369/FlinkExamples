package com.sj.flink.stream.pojo;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class OrderSkuQty {
    String skuNo;
    Integer quantity = 0;
}
