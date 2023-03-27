package com.wx2.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TumblingProcessing {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final FileSource<String> source =
                FileSource.forRecordStreamFormat(
                        new TextLineFormat(),
                                new Path("C:\\Users\\lmandle\\Desktop\\flinkpractice\\DataforFlinkP\\Tump.txt"
                                )
                        )
                        .build();
        final DataStream<String> data =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
        //data.print();
        //DataStream<String> data =
        //env.readTextFile("C:\\Users\\lmandle\\Desktop\\flinkpractice\\DataforFlinkP\\Tump.txt").print();

        // month, product, category, profit, count
        DataStream<Tuple5<String, String, String, Integer, Integer>> mapped = data.map(new Splitter());
        DataStream<Tuple5<String, String, String, Integer, Integer>> reduced = mapped
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(1)))
                .reduce(new Reduce1());
                //.uid("abcd");

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
