package flink.jdbc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

public class order_copy_test {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());

        TableResult tabsource = tabEnv.executeSql("" +
                "create table t_order_copy (\n" +
                "id BIGINT ,\n" +
                "pl string ,\n" +
                "os string ,\n" +
                "gn string ,\n" +
                "order_id string ,\n" +
                "transaction_id string ,\n" +
                "order_status SMALLINT ,\n" +
                "createtimestamp bigint ,\n" +
                "ts as TO_TIMESTAMP(FROM_UNIXTIME(createtimestamp,'yyyy-MM-dd HH:mm:ss')),\n" +
                "successtimestamp int ,\n" +
                "order_amount BIGINT ,\n" +
                "product_id string ,\n" +
                "gameMoney BIGINT ,\n" +
                "uid string ,\n" +
                "qid string ,\n" +
                "pid BIGINT ,\n" +
                "group_id string ,\n" +
                "issandbox SMALLINT ,\n" +
                "ip BIGINT ,\n" +
                "level BIGINT ,\n" +
                "vip BIGINT ,\n" +
                "uuid string ,\n" +
                "item_id string ,\n" +
                "order_type SMALLINT ,\n" +
                "uuid_create string ,\n" +
                "uuid_hash_high string ,\n" +
                "uuid_hash_low string ,\n" +
                "plosgn_reg int,\n" +
                "true_cost int ,\n" +
                "state BIGINT ,\n" +
                "is_inner SMALLINT ,\n" +
                "pay_type BIGINT ,\n" +
                "plosgn BIGINT ,\n" +
                "groupbase BIGINT ,\n" +
                "time_zone string ,\n" +
                "extra_stat string ,\n" +
                "item_version BIGINT ,\n" +
                "WATERMARK FOR ts  as ts - INTERVAL '5' SECOND,\n" +
                "tsp AS PROCTIME () ,\n" +
                "PRIMARY KEY (id) NOT ENFORCED" +
                ")\n" +
                "with\n" +
                "('connector' = 'mysql-cdc' ,\n" +
                "'hostname' = '39.106.39.121' ,\n" +
                "'port' = '3306',\n" +
                "'username'  ='root' ,\n" +
                "'password' = 'YUZ224102lss@#' ,\n" +
                "'database-name' = 'fk_test' ,\n" +
                "'table-name' = 't_ioshunfu_order_copy' )");


        TableResult tableResult = tabEnv.executeSql("select * from t_order_copy ");

        tabEnv.toAppendStream((Table) tableResult,Row.class).print() ;

        tabEnv.sqlQuery("" +
                "select\n" +
                "FROM_UNIXTIME(createtimestamp,'yyyy-MM-dd') as t,\n" +
                "groupbase,\n" +
                "SUM(true_cost) OVER\n" +
                "(PARTITION BY groupbase ORDER BY ts RANGE BETWEEN INTERVAL '1' DAY preceding AND CURRENT ROW) AS sum_cost\n" +
                "from t_order_copy").execute();




//        tabEnv.toAppendStream(sqlQuery, Row.class).print();
    }
}
