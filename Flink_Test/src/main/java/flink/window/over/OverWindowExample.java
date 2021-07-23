package flink.window.over;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.*;

public class OverWindowExample {
    public static void main(String[] args) throws Exception {

        EnvironmentSettings mysettings = EnvironmentSettings
                .newInstance()
                .useOldPlanner()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //指定系統時間為概念為 event time
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime.EventTime);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, mysettings);

        DataStreamSource<Tuple3<Long, String, Integer>> log = env.fromCollection(Arrays.asList(
                //时间 14:53:00
                new Tuple3<>(1572591180_000L,"xiao_ming",999),
                //时间 14:53:09
                new Tuple3<>(1572591189_000L,"zhang_san",303),
                //时间 14:53:12
                new Tuple3<>(1572591192_000L, "xiao_li",888),
                //时间 14:53:21
                new Tuple3<>(1572591201_000L,"li_si", 908),
                //2019-11-01 14:53:31
                new Tuple3<>(1572591211_000L,"li_si", 555),
                //2019-11-01 14:53:41
                new Tuple3<>(1572591221_000L,"zhang_san", 666),
                //2019-11-01 14:53:51
                new Tuple3<>(1572591231_000L,"xiao_ming", 777),
                //2019-11-01 14:54:01
                new Tuple3<>(1572591241_000L,"xiao_ming", 213),
                //2019-11-01 14:54:11
                new Tuple3<>(1572591251_000L,"zhang_san", 300),
                //2019-11-01 14:54:21
                new Tuple3<>(1572591261_000L,"li_si", 112)
        ));

        //指定時間戳
        //参考官网指定时间戳，直接使用lambda表达式获取
        WatermarkStrategy<Tuple3<Long, String, Integer>> withTimestampAssigner = WatermarkStrategy.<Tuple3<Long, String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner((event, timestamp) -> event.f0);

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> tuple3SingleOutputStreamOperator = log.assignTimestampsAndWatermarks(withTimestampAssigner);

        tableEnvironment.createTemporaryView("tbl",tuple3SingleOutputStreamOperator, $("t").rowtime(), $("name"), $("v")) ;

        String sql1 = "SELECT name,v,MAX(v) OVER(\n" +
                "PARTITION BY name \n" +
                "ORDER BY t \n" +
                "RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW\n" +
                ") FROM tbl"  ;

        String sql2 = "SELECT SESSION_START(t, INTERVAL '5' SECOND) AS window_start," +
                "SESSION_END(t, INTERVAL '5' SECOND) AS window_end, SUM(v) FROM "
                + "tbl" + " GROUP BY SESSION(t, INTERVAL '5' SECOND)" ;

        String sql3 = "SELECT TUMBLE_START(t, INTERVAL '10' SECOND) AS window_start," +
                "TUMBLE_END(t, INTERVAL '10' SECOND) AS window_end, SUM(v) FROM "
                + "tbl" + " GROUP BY TUMBLE(t, INTERVAL '10' SECOND)" ;


        tableEnvironment.executeSql(sql1).print();
        tableEnvironment.executeSql(sql2).print();

        Table sqlQuery = tableEnvironment.sqlQuery(sql3);

        TypeInformation<Tuple3<String, Integer, Integer>> typeInfo = new TypeHint<Tuple3<String, Integer, Integer>>() {}.getTypeInfo();

        TypeInformation<Tuple3<Timestamp,Timestamp,Integer>> tpinf = new TypeHint<Tuple3<Timestamp,Timestamp,Integer>>(){}.getTypeInfo();


        tableEnvironment.toAppendStream(sqlQuery,tpinf).print();

        env.execute();


        /**        table
         .window(Over.partitionBy($("id")).orderBy($("ts")).preceding(UNBOUNDED_ROW).as("w"))
         .select($("id"), $("ts"), $("vc").sum().over($("w")).as("sum_vc"))
         .execute()
         .print();

         */


    }

}
