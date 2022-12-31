package com.sj.flink.stream.pojo;

import lombok.Data;

@Data
public class OrderSummary {
    OrderSkuQty orderSkuQty;
    OrderClientQty orderClientQty;
}
