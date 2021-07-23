package flink.CanalBinlog;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

/**
 *  @Created with IntelliJ IDEA.
 *  @author : jmx
 *  @Date: 2020/3/27
 *  @Time: 12:52
 *
 */
public class HdfsSink {
    public static void main(String[] args) throws Exception {
        final String fieldDelimiter = ",";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        // checkpoint
//        env.enableCheckpointing(10000);
//        //env.setStateBackend((StateBackend) new FsStateBackend("file:///E://checkpoint"));
//        env.setStateBackend((StateBackend) new FsStateBackend("hdfs://kms-1:8020/checkpoint"));
//        CheckpointConfig config = env.getCheckpointConfig();
//        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

//        // source
//        Properties props = new Properties();
//        props.setProperty("bootstrap.servers", "kms-2:9092,kms-3:9092,kms-4:9092");
//        // only required for Kafka 0.8
//        props.setProperty("zookeeper.connect", "kms-2:2181,kms-3:2181,kms-4:2181");
//        props.setProperty("group.id", "test123");
//        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
//                "qfbap_ods.code_city", new SimpleStringSchema(), props);
//        consumer.setStartFromEarliest();
//        DataStream<String> stream = env.addSource(consumer);

        //source
        DataStreamSource<String> stream = env.readTextFile("input/canaljson.json");

        // transform
        SingleOutputStreamOperator<String> cityDS = stream
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String jsonVal) throws Exception {
                        JSONObject record = JSON.parseObject("{\n" +
                                "    \"data\":[\n" +
                                "        {\n" +
                                "            \"id\":\"338\",\n" +
                                "            \"city\":\"成都\",\n" +
                                "            \"province\":\"四川省\"\n" +
                                "        }\n" +
                                "    ],\n" +
                                "    \"database\":\"qfbap_ods\",\n" +
                                "    \"es\":1583394964000,\n" +
                                "    \"id\":2,\n" +
                                "    \"isDdl\":false,\n" +
                                "    \"mysqlType\":{\n" +
                                "        \"id\":\"int(11)\",\n" +
                                "        \"city\":\"varchar(256)\",\n" +
                                "        \"province\":\"varchar(256)\"\n" +
                                "    },\n" +
                                "    \"old\":null,\n" +
                                "    \"pkNames\":[\n" +
                                "        \"id\"\n" +
                                "    ],\n" +
                                "    \"sql\":\"\",\n" +
                                "    \"sqlType\":{\n" +
                                "        \"id\":4,\n" +
                                "        \"city\":12,\n" +
                                "        \"province\":12\n" +
                                "    },\n" +
                                "    \"table\":\"code_city\",\n" +
                                "    \"ts\":1583394964361,\n" +
                                "    \"type\":\"INSERT\"\n" +
                                "}", Feature.OrderedField);
                        return record.getString("isDdl").equals("false");
                    }
                })
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        StringBuilder stringBuilder = new StringBuilder() ;
                        //解析json数据
                        JSONObject record = JSON.parseObject("{\n" +
                                "    \"data\":[\n" +
                                "        {\n" +
                                "            \"id\":\"338\",\n" +
                                "            \"city\":\"成都\",\n" +
                                "            \"province\":\"四川省\"\n" +
                                "        }\n" +
                                "    ],\n" +
                                "    \"database\":\"qfbap_ods\",\n" +
                                "    \"es\":1583394964000,\n" +
                                "    \"id\":2,\n" +
                                "    \"isDdl\":false,\n" +
                                "    \"mysqlType\":{\n" +
                                "        \"id\":\"int(11)\",\n" +
                                "        \"city\":\"varchar(256)\",\n" +
                                "        \"province\":\"varchar(256)\"\n" +
                                "    },\n" +
                                "    \"old\":null,\n" +
                                "    \"pkNames\":[\n" +
                                "        \"id\"\n" +
                                "    ],\n" +
                                "    \"sql\":\"\",\n" +
                                "    \"sqlType\":{\n" +
                                "        \"id\":4,\n" +
                                "        \"city\":12,\n" +
                                "        \"province\":12\n" +
                                "    },\n" +
                                "    \"table\":\"code_city\",\n" +
                                "    \"ts\":1583394964361,\n" +
                                "    \"type\":\"INSERT\"\n" +
                                "}", Feature.OrderedField);

                        //获取最新的字段值
                        JSONArray data = record.getJSONArray("data");

                        //解析多级json串
                        JSONObject sqlType = (JSONObject) record.get("sqlType");
                        Object id = sqlType.get("id");

                        //遍历字段值的json数组，只有一个元素
                        for(int i=0 ;i<data.size();i++){
                            JSONObject obj = data.getJSONObject(i);

                            if (obj != null){
                                stringBuilder.append(record.getLong("id")) ; //序号id
                                stringBuilder.append(fieldDelimiter) ; //字段分割符
                                stringBuilder.append(record.getLong("es")) ; //业务时间戳
                                stringBuilder.append(fieldDelimiter) ;
                                stringBuilder.append(record.getLong("ts")) ; //日志时间戳
                                stringBuilder.append(fieldDelimiter) ;
                                stringBuilder.append(record.getString("type")) ; //操作类型

                                stringBuilder.append("-->"+id) ;
                                record.getString("sqlType") ;


                                for (Map.Entry<String,Object> entry : obj.entrySet()) {
                                    stringBuilder.append(fieldDelimiter) ;
                                    stringBuilder.append(entry.getKey()+"-->"+entry.getValue()) ; //表字段数据
                                }
                            }
                        }
                        System.out.println("==============json数组的长度是==================： "+data.size());

                        return stringBuilder.toString();
                    }
                });



        cityDS.print();
//        stream.print();

        // sink
        // 以下条件满足其中之一就会滚动生成新的文件
//        RollingPolicy<String, String> rollingPolicy = DefaultRollingPolicy.create()
//                .withRolloverInterval(60L * 1000L) //滚动写入新文件的时间，默认60s。根据具体情况调节
//                .withMaxPartSize(1024 * 1024 * 128L) //设置每个文件的最大大小 ,默认是128M，这里设置为128M
//                .withInactivityInterval(60L * 1000L) //默认60秒,未写入数据处于不活跃状态超时会滚动新文件
//                .build();

//        StreamingFileSink<String> sink = StreamingFileSink
//                //.forRowFormat(new Path("file:///E://binlog_db/city"), new SimpleStringEncoder<String>())
//                .forRowFormat(new Path("hdfs://kms-1:8020/binlog_db/code_city_delta"), new SimpleStringEncoder<String>())
//                .withBucketAssigner(new EventTimeBucketAssigner())
//                .withRollingPolicy(rollingPolicy)
//                .withBucketCheckInterval(1000)  // 桶检查间隔，这里设置1S
//                .build();
//
//        cityDS.addSink(sink);
        env.execute();
    }
}