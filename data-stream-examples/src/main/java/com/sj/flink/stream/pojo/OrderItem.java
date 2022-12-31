package com.sj.flink.stream.pojo;

import lombok.Builder;
import lombok.Data;

import java.sql.Timestamp;

@Data
public class OrderItem {
    String skuNo;
    String client;
    Timestamp timestamp;
    Integer quantity;

    public OrderItem(String value){
        String[] data = value.split(" ");
        this.skuNo = data[0];
        this.client = data[1];
        this.timestamp = new Timestamp(Long.parseLong(data[2]));
        this.quantity = Integer.parseInt(data[3]);
    }
}
