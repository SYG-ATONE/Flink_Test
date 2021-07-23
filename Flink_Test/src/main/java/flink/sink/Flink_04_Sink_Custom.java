package flink.sink;

import lombok.val;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;

public class Flink_04_Sink_Custom {
    public static void main(String[] args) throws Exception {
        ArrayList<WaterSensor> waterSensors = new ArrayList<WaterSensor>() ;

        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment() ;
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        env.fromElements(waterSensors)
                .addSink(new RichSinkFunction<ArrayList<WaterSensor>>() {

                    private PreparedStatement ps ;
                    private Connection conn ;

                    //初始化，创建连接、和预编译语句
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        conn = DriverManager.getConnection("jdbc:mysql://39.106.39.121:3306/fk_test?useSSL=false", "root", "YUZ224102lss@#") ;
//                        conn.setAutoCommit(false);
                        ps = conn.prepareStatement("insert into sensor values(?, ?, ?)");

                    }

                    //关闭资源及清理工作
                    @Override
                    public void close() throws Exception {
//                        conn.commit();
                        conn.close();
                        ps.close();
                        super.close();
                    }

                    //调用连接，执行sql
                    public void invoke(ArrayList<WaterSensor> value, Context context) throws Exception {

                        for (int i=0;i<value.size();i++ ){

                            ps.setString(1, String.valueOf(value.get(i).getId()));
                            ps.setLong(2,value.get(i).getTs());
                            ps.setInt(3,value.get(i).getVc());
                            System.out.println(ps);
                            ps.execute();

                        }


                    }
                }) ;

        env.execute() ;

    }
}
