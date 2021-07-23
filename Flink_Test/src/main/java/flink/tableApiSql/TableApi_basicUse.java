package flink.tableApiSql;

import flink.sink.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class TableApi_basicUse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1) ;
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60)
        );

        //创建表环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        //创建表，将流转换为动态表，表字段的名字从pojo的属性中自动抽取
        Table table = tableEnvironment.fromDataStream(waterSensorDataStreamSource);

        //对动态表进行查询
        Table tableResult = table.where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"));
        //把动态表转换成流
        DataStream<Row> dataStream = tableEnvironment.toAppendStream(tableResult, Row.class);
        System.out.println("table表结果"+tableResult);




        try {
            env.execute() ;
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
