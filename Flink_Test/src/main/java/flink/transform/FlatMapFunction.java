package flink.transform;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMapFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(new Work("Hello",2),new Work("Hi",5))
                .flatMap(new org.apache.flink.api.common.functions.FlatMapFunction<Work, String>() {

                    public void flatMap(Work work, Collector<String> collector) throws Exception {
                        collector.collect("基本信息"+"===》"+work);
                    }
                }).print();
    }
}



