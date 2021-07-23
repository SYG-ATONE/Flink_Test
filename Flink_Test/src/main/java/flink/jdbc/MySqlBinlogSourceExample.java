package flink.jdbc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MySqlBinlogSourceExample {
    public static void main(String[] args) throws Exception {

        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("39.106.39.121")
                .port(3306)
                .databaseList("fk_test")
                .username("root")
                .password("YUZ224102lss@#")
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(sourceFunction)
                .print().setParallelism(1);




        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().build());
        tableEnv.executeSql("create table sensor(id string,ts bigint,vc int,pt_time as PROCTIME()) " +
                "with" +
                "('connector' = 'mysql-cdc',\n" +
                "  'hostname' = '39.106.39.121',\n" +
                "  'port' = '3306',\n" +
                "  'username' = 'root',\n" +
                "  'password' = 'YUZ224102lss@#',\n" +
                "  'database-name' = 'fk_test',\n" +
                "  'table-name' = 'sensor')");


//        tableEnv.executeSql("select * from sensor").print();
        tableEnv.executeSql("select * from sensor where id = 'sensor_1'").print(); ;

        env.execute();

    }
}
