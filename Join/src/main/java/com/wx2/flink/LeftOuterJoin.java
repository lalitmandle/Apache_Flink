package com.wx2.flink;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class LeftOuterJoin {
    public static void main(String[] args) throws Exception
    {
        ExecutionEnvironment evn = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Integer,String>> personSet = evn.readTextFile("C:\\Users\\lmandle\\Desktop\\JOINS\\person")
                .map(new MapFunction<String, Tuple2<Integer, String>>() {
                    public Tuple2<Integer, String> map(String value)
                    {
                        String[] words = value.split(",");
                        return new Tuple2<Integer,String>(Integer.parseInt(words[0]),words[1]);
                    }
                });

        DataSet<Tuple2<Integer,String>> locationSet = evn.readTextFile("C:\\Users\\lmandle\\Desktop\\JOINS\\location")
                .map(new MapFunction<String, Tuple2<Integer, String>>() {
                    public Tuple2<Integer, String> map(String value)
                    {
                        String[] words = value.split(",");
                        return new Tuple2<Integer,String>(Integer.parseInt(words[0]),words[1]);
                    }
                });
        //left outer join datasets on person_id
        DataSet<Tuple3<Integer, String, String>> joined = personSet.leftOuterJoin(locationSet).where(0) .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>()
                {
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person, Tuple2<Integer,String> location)
                    {
                        if (location == null)
                        {
                            return new Tuple3<Integer,String,String >(person.f0,person.f1,"NULL");
                        }
                        return new Tuple3<Integer,String ,String>(person.f0,person.f1,location.f1);
                    }
                });
        joined.print();
    }
}
