package net.nanquanyuhao.marketanalysis;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: UserBehaviorAnalysis
 * Package: com.atguigu.market_analysis
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/11/17 10:38
 */

import net.nanquanyuhao.marketanalysis.beans.AdClickEvent;
import net.nanquanyuhao.marketanalysis.beans.AdCountViewByProvince;
import net.nanquanyuhao.marketanalysis.beans.BlackListUserWarning;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.sql.Timestamp;
import java.time.Duration;

/**
 * @ClassName: AdStatisticsByProvince
 * @Description:
 * @Author: wushengran on 2020/11/17 10:38
 * @Version: 1.0
 */
public class AdStatisticsByProvince {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 1. 从文件中读取数据
        URL resource = AdStatisticsByProvince.class.getResource("/AdClickLog.csv");
        DataStream<AdClickEvent> adClickEventStream = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new AdClickEvent(new Long(fields[0]), new Long(fields[1]), fields[2], fields[3], new Long(fields[4]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<AdClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<AdClickEvent>() {
                                    @Override
                                    public long extractTimestamp(AdClickEvent adClickEvent, long l) {
                                        return adClickEvent.getTimestamp() * 1000L;
                                    }
                                }));

        // 2. 对同一个用户点击同一个广告的行为进行检测报警
        SingleOutputStreamOperator<AdClickEvent> filterAdClickStream = adClickEventStream
                //.keyBy("userId", "adId")    // 基于用户id和广告id做分组
                .keyBy(new KeySelector<AdClickEvent, Tuple2<Long, Long>>() {

                    @Override
                    public Tuple2<Long, Long> getKey(AdClickEvent adClickEvent) throws Exception {
                        return new Tuple2<>(adClickEvent.getUserId(), adClickEvent.getAdId());
                    }
                })
                .process(new FilterBlackListUser(100));

        // 3. 基于省份分组，开窗聚合
        SingleOutputStreamOperator<AdCountViewByProvince> adCountResultStream = filterAdClickStream
                .keyBy(AdClickEvent::getProvince)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(5)))     // 定义滑窗，5分钟输出一次
                .aggregate(new AdCountAgg(), new AdCountResult());

        adCountResultStream.print();
        filterAdClickStream.getSideOutput(new OutputTag<BlackListUserWarning>("blacklist") {
        }).print("blacklist-user");

        env.execute("ad count by province job");
    }

    public static class AdCountAgg implements AggregateFunction<AdClickEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    public static class AdCountResult implements WindowFunction<Long, AdCountViewByProvince, String, TimeWindow> {
        @Override
        public void apply(String province, TimeWindow window, Iterable<Long> input, Collector<AdCountViewByProvince> out) throws Exception {
            String windowEnd = new Timestamp(window.getEnd()).toString();
            Long count = input.iterator().next();
            out.collect(new AdCountViewByProvince(province, windowEnd, count));
        }
    }

    // 实现自定义处理函数
    public static class FilterBlackListUser extends KeyedProcessFunction<Tuple2<Long, Long>, AdClickEvent, AdClickEvent> {
        // 定义属性：点击次数上限
        private Integer countUpperBound;

        public FilterBlackListUser(Integer countUpperBound) {
            this.countUpperBound = countUpperBound;
        }

        // 定义状态，保存当前用户对某一广告的点击次数
        ValueState<Long> countState;
        // 定义一个标志状态，保存当前用户是否已经被发送到了黑名单里
        ValueState<Boolean> isSentState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ad-count", Long.class));
            isSentState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-sent", Boolean.class));
        }

        @Override
        public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {

            // 判断当前用户对同一广告的点击次数，如果不够上限，就count加1正常输出；如果达到上限，直接过滤掉，并侧输出流输出黑名单报警
            // 首先获取当前的count值
            Long curCount = countState.value();
            if (curCount == null) {
                curCount = 0L;
            }

            // 1. 判断是否是第一个数据，如果是的话，注册一个第二天0点的定时器
            if (curCount == 0) {
                // 使用事件时间语义，即使用水位时间而且处理时间
                Long ts = (ctx.timerService().currentWatermark() / (24 * 60 * 60 * 1000) + 1) * (24 * 60 * 60 * 1000) - 8 * 60 * 60 * 1000;
//                System.out.println(new Timestamp(ts));
                // 以下需要使用事件时间定时器进行注册
                //ctx.timerService().registerProcessingTimeTimer(ts);
                ctx.timerService().registerEventTimeTimer(ts);
            }

            // 2. 判断是否报警
            if (curCount >= countUpperBound) {
                if (isSentState.value() == null) {
                    isSentState.update(false);
                }
                // 判断是否输出到黑名单过，如果没有的话就输出到侧输出流
                if (!isSentState.value()) {
                    isSentState.update(true);    // 更新状态
                    ctx.output(new OutputTag<BlackListUserWarning>("blacklist") {
                               },
                            new BlackListUserWarning(value.getUserId(), value.getAdId(), "click over " + countUpperBound + "times."));
                }
                return;    // 不再执行下面操作
            }

            // 如果没有返回，点击次数加1，更新状态，正常输出当前数据到主流
            countState.update(curCount + 1);
            out.collect(value);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            // 清空所有状态
            countState.clear();
            isSentState.clear();
        }
    }
}
