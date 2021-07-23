package flink.jdbc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class OrderInfo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().build());

        TableResult order_info = tableEnvironment.executeSql(
                "CREATE TABLE order_info(\n" +
                        "    id BIGINT,\n" +
                        "    user_id BIGINT,\n" +
                        "    create_time TIMESTAMP(0),\n" +
                        "    operate_time TIMESTAMP(0),\n" +
                        "    province_id INT,\n" +
                        "    order_status STRING,\n" +
                        "    total_amount DECIMAL(10, 5)\n" +
                        "  ) WITH (\n" +
                        "    'connector' = 'mysql-cdc',\n" +
                        "    'hostname' = '39.106.39.121',\n" +
                        "    'port' = '3306',\n" +
                        "    'username' = 'root',\n" +
                        "    'password' = 'YUZ224102lss@#',\n" +
                        "    'database-name' = 'fk_test',\n" +
                        "    'table-name' = 'order_info'\n" +
                        ")"
        );


        TableResult order_detail = tableEnvironment.executeSql(
                "CREATE TABLE order_detail(\n" +
                        "    id BIGINT,\n" +
                        "    order_id BIGINT,\n" +
                        "    sku_id BIGINT,\n" +
                        "    sku_name STRING,\n" +
                        "    sku_num BIGINT,\n" +
                        "    order_price DECIMAL(10, 5),\n" +
                        "    create_time TIMESTAMP(0)\n" +
                        " ) WITH (\n" +
                        "    'connector' = 'mysql-cdc',\n" +
                        "    'hostname' = '39.106.39.121',\n" +
                        "    'port' = '3306',\n" +
                        "    'username' = 'root',\n" +
                        "    'password' = 'YUZ224102lss@#',\n" +
                        "    'database-name' = 'fk_test',\n" +
                        "    'table-name' = 'order_detail'\n" +
                        ")\n"
        );

//        tableEnvironment.executeSql("select * from order_info").print();
//        tableEnvironment.executeSql("select * from order_detail").print();


//         tableEnvironment.executeSql(
//                "CREATE TABLE `order_result` (\n" +
//                        "     `id` BIGINT PRIMARY KEY NOT ENFORCED,\n" +
//                        "     `order_id` bigint,\n" +
//                        "     `user_id` bigint ,\n" +
//                        "     province_id int ,\n" +
//                        "     sku_id bigint ,\n" +
//                        "     sku_name string ,\n" +
//                        "     sku_num bigint ,\n" +
//                        "     order_price decimal(10,2),\n" +
//                        "     create_time TIMESTAMP(0) ,\n" +
//                        "     opreate_time date  \n" +
//                        ") WITH (\n" +
//                        "   'connector' = 'jdbc',\n" +
//                        "   'driver'='com.mysql.jdbc.Driver',"+
//                        "   'username' = 'root',       -- 数据库访问的用户名（需要提供 INSERT 权限）\n" +
//                        "   'password' = 'YUZ224102lss@#' ,  -- 数据库访问的密码\n" +
//                        "   'url' = 'jdbc:mysql://39.106.39.121:3306/fk_test?useSSL=false&autoReconnect=true', -- 请替换为您的实际 MySQL 连接参数\n" +
//                        "   'table-name' = 'order_result',  -- 需要写入的数据表\n" +
//                        "   'sink.buffer-flush.max-rows' = '200',  -- 批量输出的条数\n" +
//                        "   'sink.buffer-flush.interval' = '2s'    -- 批量输出的间隔\n" +
//                        ") "
//        );

        tableEnvironment.executeSql(
                "CREATE TABLE `order_result` (\n" +
                        "     `id` string PRIMARY KEY NOT ENFORCED,\n" +
                        "     `order_id` string,\n" +
                        "     `user_id` string ,\n" +
                        "     province_id string ,\n" +
                        "     sku_id string ,\n" +
                        "     sku_name string ,\n" +
                        "     sku_num string ,\n" +
                        "     order_price string,\n" +
                        "     create_time string ,\n" +
                        "     opreate_time string  \n" +
                        ") WITH (\n" +
                        "   'connector' = 'jdbc',\n" +
                        "   'url' = 'jdbc:mysql://39.106.39.121:3306/fk_test?useSSL=false&autoReconnect=true', -- 请替换为您的实际 MySQL 连接参数\n" +
                        "   'table-name' = 'order_result',  -- 需要写入的数据表\n" +
                        "   'username' = 'root',       -- 数据库访问的用户名（需要提供 INSERT 权限）\n" +
                        "   'password' = 'YUZ224102lss@#' ,  -- 数据库访问的密码\n" +
                        "   'sink.buffer-flush.max-rows' = '200',  -- 批量输出的条数\n" +
                        "   'sink.buffer-flush.interval' = '2s'    -- 批量输出的间隔\n" +
                        ") "
        );




         tableEnvironment.executeSql(
                 "INSERT INTO order_result " +
                 "SELECT\n" +
                 "    cast(od.id as string) as id,\n" +
                 "    cast(oi.id as string) order_id,\n" +
                 "    cast(oi.user_id as string) user_id,\n" +
                 "    cast(oi.province_id as string) province_id,\n" +
                 "    cast(od.sku_id as string) sku_id,\n" +
                 "    cast(od.sku_name as string) sku_name,\n" +
                 "    cast(od.sku_num as string) sku_num,\n" +
                 "    cast(od.order_price as string) order_price,\n" +
                 "    cast(oi.create_time as string) create_time,\n" +
                 "    cast(oi.operate_time as string) as operate\n" +
                 "FROM\n" +
                 "   (\n" +
                 "    SELECT * \n" +
                 "    FROM order_info\n" +
                 "    WHERE \n" +
                 "         order_status = '1' -- 已支付\n" +
                 "   ) oi\n" +
                 "   JOIN\n" +
                 "  (\n" +
                 "    SELECT *\n" +
                 "    FROM order_detail\n" +
                 "  ) od \n" +
                 "  ON oi.id = od.order_id");


//        StatementSet statementSet = tableEnvironment.createStatementSet();

//        statementSet.addInsertSql(
//                "INSERT INTO order_result " +
//                        "SELECT\n" +
//                        "    od.id,\n" +
//                        "    oi.id order_id,\n" +
//                        "    oi.user_id,\n" +
//                        "    oi.province_id,\n" +
//                        "    od.sku_id,\n" +
//                        "    od.sku_name,\n" +
//                        "    od.sku_num,\n" +
//                        "    od.order_price,\n" +
//                        "    oi.create_time,\n" +
//                        "    cast(oi.operate_time as date)\n" +
//                        "FROM\n" +
//                        "   (\n" +
//                        "    SELECT * \n" +
//                        "    FROM order_info\n" +
//                        "    WHERE \n" +
//                        "         order_status = '2'-- 已支付\n" +
//                        "   ) oi\n" +
//                        "   JOIN\n" +
//                        "  (\n" +
//                        "    SELECT *\n" +
//                        "    FROM order_detail\n" +
//                        "  ) od \n" +
//                        "  ON oi.id = od.order_id"
//        ) ;
//
//        statementSet.execute();

        String insert_test = ("INSERT INTO order_result VALUES ('1','2','3','4','5','6','7','8','2021-03-10','10') ");

        tableEnvironment.executeSql(insert_test) ;

        tableEnvironment.sqlQuery("select * from order_result").execute().print();
//        env.execute();

    }
}
