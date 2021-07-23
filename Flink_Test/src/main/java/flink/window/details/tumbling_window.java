package flink.window.details;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class tumbling_window {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment() ;

        String hostname = "192.168.2.21" ;
        int port =9999 ;
        env.socketTextStream(hostname,port)
                .flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        String[] split = s.split("\\w+");

                        for (int i=0; i<split.length; i++){
                            collector.collect(Tuple2.of(split[i],1L));
                        }

//                        Arrays.stream(s.split("\\w+")).forEach(x -> collector.collect(Tuple2.of(x,1L)));
                        //数组使用工具类进行遍历
//                        collector.collect(Tuple2.of(Arrays.toString(split),1L)) ;
                    }
                })
                .keyBy(value -> value.f0)
//                .window(TumblingEventTimeWindows.of(Time.seconds(3))) //添加滚动窗口
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5))) //添加滑动窗口
                .sum(1)
                .print() ;


        env.execute();

    }
}
