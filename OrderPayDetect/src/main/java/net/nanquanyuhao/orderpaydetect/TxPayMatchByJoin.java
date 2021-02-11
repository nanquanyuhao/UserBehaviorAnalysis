package net.nanquanyuhao.orderpaydetect;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: UserBehaviorAnalysis
 * Package: com.atguigu.orderpay_detect
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/11/18 15:42
 */

import net.nanquanyuhao.orderpaydetect.beans.OrderEvent;
import net.nanquanyuhao.orderpaydetect.beans.ReceiptEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URL;

/**
 * @ClassName: TxPayMatchByJoin
 * @Description:
 * @Author: wushengran on 2020/11/18 15:42
 * @Version: 1.0
 */
public class TxPayMatchByJoin {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 读取数据并转换成POJO类型
        // 读取订单支付事件数据
        URL orderResource = TxPayMatchByJoin.class.getResource("/OrderLog.csv");
        DataStream<OrderEvent> orderEventStream = env.readTextFile(orderResource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                                    @Override
                                    public long extractTimestamp(OrderEvent orderEvent, long l) {
                                        return orderEvent.getTimestamp() * 1000L;
                                    }
                                }))
                .filter(data -> !"".equals(data.getTxId()));    // 交易id不为空，必须是pay事件

        // 读取到账事件数据
        URL receiptResource = TxPayMatchByJoin.class.getResource("/ReceiptLog.csv");
        SingleOutputStreamOperator<ReceiptEvent> receiptEventStream = env.readTextFile(receiptResource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new ReceiptEvent(fields[0], fields[1], new Long(fields[2]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ReceiptEvent>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<ReceiptEvent>() {
                                    @Override
                                    public long extractTimestamp(ReceiptEvent receiptEvent, long l) {
                                        return receiptEvent.getTimestamp() * 1000L;
                                    }
                                }));

        // 区间连接两条流，得到匹配的数据
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> resultStream = orderEventStream
                .keyBy(OrderEvent::getTxId)
                .intervalJoin(receiptEventStream.keyBy(ReceiptEvent::getTxId))
                .between(Time.seconds(-3), Time.seconds(5))    // -3，5 区间范围
                .process(new TxPayMatchDetectByJoin());

        resultStream.print();

        env.execute("tx pay match by join job");
    }

    // 实现自定义ProcessJoinFunction
    public static class TxPayMatchDetectByJoin extends ProcessJoinFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {
        @Override
        public void processElement(OrderEvent left, ReceiptEvent right, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            out.collect(new Tuple2<>(left, right));
        }
    }
}
