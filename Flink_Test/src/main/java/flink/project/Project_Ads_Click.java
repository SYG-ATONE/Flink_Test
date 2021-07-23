package flink.project;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.api.common.typeinfo.Types.LONG;
import static org.apache.flink.api.common.typeinfo.Types.STRING;

public class Project_Ads_Click {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment() ;
        env.readTextFile("input/AdClickLog.csv")
                .map(new MapFunction<String, AdsClickLog>() {
                    public AdsClickLog map(String line) throws Exception {
                        String[] split = line.split(",");
                        return new AdsClickLog(
                                Long.valueOf(split[0]),
                                Long.valueOf(split[1]),
                                split[2],
                                split[3],
                                Long.valueOf(split[4]));
                    }
                }).map(log -> Tuple2.of(Tuple2.of(log.getProvince(),log.getAdId()),1L))
                .returns(Types.TUPLE(Types.TUPLE(STRING,LONG), LONG))
               // .keyBy(Project_Ads_Click::getKey)   //这个采用静态方法进行调用
                .keyBy(new MyKeyBySelector())        //通过实现KeySelector接口解决
                .sum(1)
                .print("Province--Ad") ;
        env.execute();
    }

    private static Tuple2<String, Long> getKey(Tuple2<Tuple2<String, Long>, Long> value) {
        return value.f0;
    }

    static class MyKeyBySelector implements KeySelector<Tuple2<Tuple2<String,Long>,Long>,Tuple2<String,Long>>{
        @Override
        public Tuple2<String, Long> getKey(Tuple2<Tuple2<String, Long>, Long> value) throws Exception {
            return value.f0 ;
        }
    }

}
