package net.nanquanyuhao.networkanalysis;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: UserBehaviorAnalysis
 * Package: com.atguigu.networkflow_analysis
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/11/16 11:46
 */

import net.nanquanyuhao.networkanalysis.beans.PageViewCount;
import net.nanquanyuhao.networkanalysis.beans.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.Random;

/**
 * @ClassName: PageView
 * @Description:
 * @Author: wushengran on 2020/11/16 11:46
 * @Version: 1.0
 */
public class PageView {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 读取数据，创建DataStream
        URL resource = PageView.class.getResource("/UserBehavior.csv");
        DataStream<String> inputStream = env.readTextFile(resource.getPath());

        // 3. 转换为POJO，分配时间戳和watermark
        DataStream<UserBehavior> dataStream = inputStream
                .map(line -> {
                    String[] fields = line.split(",");
                    return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior var1, long var2) {
                                        return var1.getTimestamp() * 1000L;
                                    }
                                }));

        // 4. 分组开窗聚合，得到每个窗口内各个商品的count值
        SingleOutputStreamOperator<Tuple2<String, Long>> pvResultStream0 =
                dataStream
                        .filter(data -> "pv".equals(data.getBehavior()))    // 过滤pv行为
                        .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                            @Override
                            public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                                // 开窗前必须先分组，故重组数据结构，默认对同一值“pv”进行分组
                                return new Tuple2<>("pv", 1L);
                            }
                        })
                        .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                            @Override
                            public String getKey(Tuple2<String, Long> stringLongTuple2) throws Exception {
                                return stringLongTuple2.f0;
                            }
                        })    // 按商品ID分组
                        .window(TumblingEventTimeWindows.of(Time.hours(1)))    // 开1小时滚动窗口
                        .sum(1);

        //  并行任务改进，设计随机key，解决数据倾斜问题
        SingleOutputStreamOperator<PageViewCount> pvStream = dataStream.filter(data -> "pv".equals(data.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> map(UserBehavior value) throws Exception {
                        Random random = new Random();
                        // 开窗前必须先分组，重组数据结构，使用不同键
                        return new Tuple2<>(random.nextInt(10), 1L);
                    }
                })
                .keyBy(new KeySelector<Tuple2<Integer, Long>, Integer>() {
                    @Override
                    public Integer getKey(Tuple2<Integer, Long> integerLongTuple2) throws Exception {
                        return integerLongTuple2.f0;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new PvCountAgg(), new PvCountResult());

        // 将各分区数据依照窗口关闭时间汇总起来，多并行分支下最终需要结果合并
        DataStream<PageViewCount> pvResultStream = pvStream
                .keyBy(new KeySelector<PageViewCount, Long>() {
                    @Override
                    public Long getKey(PageViewCount pageViewCount) throws Exception {
                        return pageViewCount.getWindowEnd();
                    }
                })
                .process(new TotalPvCount());
//                .sum("count");

        pvResultStream.print();

        env.execute("pv count job");
    }

    // 实现自定义预聚合函数
    public static class PvCountAgg implements AggregateFunction<Tuple2<Integer, Long>, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Integer, Long> value, Long accumulator) {
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

    // 实现自定义窗口
    public static class PvCountResult implements WindowFunction<Long, PageViewCount, Integer, TimeWindow> {
        @Override
        public void apply(Integer integer, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            out.collect(new PageViewCount(integer.toString(), window.getEnd(), input.iterator().next()));
        }
    }

    // 实现自定义处理函数，把相同窗口分组统计的count值叠加
    public static class TotalPvCount extends KeyedProcessFunction<Long, PageViewCount, PageViewCount> {
        // 定义状态，保存当前的总count值
        ValueState<Long> totalCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            totalCountState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("total-count", Long.class));
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<PageViewCount> out) throws Exception {
            if (totalCountState.value() == null) {
                totalCountState.update(0L);
            }
            totalCountState.update(totalCountState.value() + value.getCount());
            // 注册定时器，时间窗口到达 1ms 后触发 onTimer
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
            // 定时器触发，所有分组count值都到齐，直接输出当前的总count数量
            Long totalCount = totalCountState.value();
            out.collect(new PageViewCount("pv", ctx.getCurrentKey(), totalCount));
            // 清空状态
            totalCountState.clear();
        }
    }
}
