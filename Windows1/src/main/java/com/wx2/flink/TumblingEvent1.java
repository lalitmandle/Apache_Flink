package com.wx2.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TumblingEvent1 {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        WatermarkStrategy<Tuple2< Long, String >> ws =
                WatermarkStrategy
                        . < Tuple2 < Long, String >> forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.f0);

        DataStream<String> data = env.readTextFile("C:\\Users\\lmandle\\Desktop\\flinkpractice\\DataforFlinkP\\Tump.txt");

        // month, product, category, profit, count
        DataStream<Tuple5<String, String, String, Integer, Integer>> mapped = data.map(new Splitter());
        DataStream<Tuple5<String, String, String, Integer, Integer>> reduced = mapped
//                .keyBy(0)
                .assignTimestampsAndWatermarks((AssignerWithPeriodicWatermarks<Tuple5<String, String, String, Integer, Integer>>) ws)
                .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(2)))
                .reduce(new Reduce1());

        reduced.print();
        env.execute();
    }


    public static  class Reduce1 implements ReduceFunction<Tuple5<String,String,String,Integer,Integer>>
    {
        public Tuple5<String, String, String, Integer, Integer> reduce(Tuple5<String, String, String, Integer, Integer> current,
                                                                       Tuple5<String, String, String, Integer, Integer> pre_result)
        {
            return new Tuple5<String,String,String,Integer,Integer>(current.f0,current.f1,current.f2,current.f3+pre_result.f3,current.f4+pre_result.f4);
        }

    }

    public static class Splitter implements MapFunction<String,Tuple5<String,String,String,Integer,Integer>>
    {
        public Tuple5<String,String,String,Integer,Integer> map(String value)
        {
            String[] words = value.split(",");
            return new Tuple5<String, String,String,Integer,Integer>(words[1], words[2],	words[3], Integer.parseInt(words[4]), 1);
        }
    }
}
