package project_test1.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import project_test1.gmall.realtime.utils.MyKafkaUtil;

public class BaseDBApp {
    public static void main(String[] args) throws Exception {

        //TODO 0.基本环境准备
        //flink流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(6) ;

        //设置CK相关参数
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE) ;
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://mycluster/gmall/flink/checkpoint"));
        System.setProperty("HADOOP_USER_NAME","atone");

        //TODO 1.接收Kafka数据，过滤空值数据
        //定义消费者组及消费主题
        String topic = "ods_base_db_m" ;
        String groupId = "ods_base_group" ;

        //从kafka主题中读取数据、
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> jsonDstream = env.addSource(kafkaSource);
//        jsonDstream.print("data json :::::::");

        //对数据进行转换 String->JSONObject
        DataStream<JSONObject> jsonStream = jsonDstream.map(jsonStr -> JSON.parseObject(jsonStr));
//        SingleOutputStreamOperator<JSONObject> json2 = jsonDstream.map(JSON::parseObject); //第二种方式获取

        //过滤为空或者长度不足的数据
        SingleOutputStreamOperator<JSONObject> filteredDstream = jsonStream.filter(
                jsonObject -> {
                    boolean flag = jsonObject.getString("table") != null
                            && jsonObject.getJSONObject("data") != null
                            && jsonObject.getString("data").length() > 3;
                    return flag;
                }
        );
        filteredDstream.print("json:::::::::::");

        env.execute();

    }
}
