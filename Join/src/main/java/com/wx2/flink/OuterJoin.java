package com.wx2.flink;


import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

@SuppressWarnings("serial")
public class OuterJoin {
    public static void main(String[] args) throws Exception {
        //set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        
        DataSet<Tuple2<Integer, String>> personSet = env.readTextFile("C:\\Users\\lmandle\\Desktop\\JOINS\\person")
                .map(new MapFunction<String, Tuple2<Integer,String>>() {
                    public Tuple2<Integer,String> map(String value)
                    {
                        String[] words = value.split(",");
                        return new Tuple2<Integer,String>(Integer.parseInt(words[0]),words[1]);
                    }
                });
        DataSet<Tuple2<Integer,String>> locationSet = env.readTextFile("C:\\Users\\lmandle\\Desktop\\JOINS\\location")
                .map(new MapFunction<String, Tuple2<Integer, String>>() {
                    public Tuple2<Integer,String> map(String value)
                    {
                        String[] words = value.split(",");
                        return new Tuple2<Integer,String>(Integer.parseInt(words[0]),words[1]);
                    }
                });
        //right outer join database on persion_id
        //joined formate will be <id,person_name,state>
        DataSet<Tuple3<Integer,String,String>> joined = personSet.fullOuterJoin(locationSet).where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person, Tuple2<Integer,String> location)
                    {
                        if (location == null){
                            return new Tuple3<Integer,String,String>(person.f0,person.f1,"NULL");
                        }
                        else if (person == null)
                            return new Tuple3<Integer, String,String>(location.f0, "NULL",location.f1);
                            return new Tuple3<Integer,String ,String >(person.f0,person.f1,location.f1);
                    }
                });
        joined.print();
    }
}
