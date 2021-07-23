//package flink.kafka;
///**
// * 具体的两阶段提交步骤总结如下：
// * 1)第一条数据来了之后，开启一个kafka 的事务（transaction），正常写入kafka 分区日志但标记为未提交，这就是“预提交”
// * 2)jobmanager 触发checkpoint 操作，barrier 从source 开始向下传递，遇到barrier 的算子将状态存入状态后端，并通知jobmanagerr
// * 3)sink 连接器收到barrier，保存当前状态，存入checkpoint，通知jobmanager，并开启下一阶段的事务，用于提交下个检查点的数据
// * 4)jobmanager 收到所有任务的通知，发出确认信息，表示checkpoint 完成
// * 5)sink 任务收到jobmanager 的确认信息，正式提交这段时间的数据
// * 6)外部kafka关闭事务，提交的数据可以正常消费了
// */
//
//import flink.sink.WaterSensor;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
//import org.apache.flink.streaming.api.CheckpointingMode;
//import org.apache.flink.streaming.api.environment.CheckpointConfig;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
//import org.apache.flink.util.Collector;
//
//import java.util.Properties;
//
//public class Flink04_State_Checkpoint {
//    public static void main(String[] args) throws Exception {
//        System.setProperty("HADOOP_USER_NAME", "atguigu");
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
//        properties.setProperty("group.id", "Flink01_Source_Kafka");
//        properties.setProperty("auto.offset.reset", "latest");
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration()).setParallelism(3);
//
//        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8020/flink/checkpoints/rocksdb"));
//
//        // 每1000ms 开始一次checkpoint
//         env.enableCheckpointing(1000);
//        // 高级选项：
//        // 设置模式为精确一次(这是默认值)
//         env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//         // 确认checkpoints 之间的时间会进行500 ms
//         env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
//         // Checkpoint 必须在一分钟内完成，否则就会被抛弃
//         env.getCheckpointConfig().setCheckpointTimeout(60000);
//         // 同一时间只允许一个checkpoint 进行
//         env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//         // 开启在job 中止后仍然保留的externalized checkpoints
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), properties)).map(value -> {
//            String[] datas = value.split(",");
//            return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
//        }).keyBy(WaterSensor::getId).process(new KeyedProcessFunction<String, WaterSensor, String>() {
//            private ValueState<Integer> state;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                getRuntimeContext().getState(new ValueStateDescriptor<Integer>("state",Integer.class)) ;
//            }
//
//            @Override
//            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
//                Integer lastVc = state.value() == null ? 0 : state.value();
//                if (Math.abs(value.getVc())-lastVc>=10){
//                    out.collect(value.getId()+"红色报警！！！");
//                }
//                state.update(value.getVc());
//            }
//
//            //            @Overridepublic
////            public void open(Configuration parameters) throws Exception {
////                state = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("state", Integer.class));
////            }
//
//
////            @Overridepublic
////            void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
////                Integer lastVc = state.value() == null ? 0 : state.value();
////                if (Math.abs(value.getVc() - lastVc) >= 10) {
////                    out.collect(value.getId() + " 红色警报!!!");
////                }
////                state.update(value.getVc());
////            }
//        }).addSink(new FlinkKafkaProducer<String>("hadoop102:9092", "alert", new SimpleStringSchema()));
//        env.execute();
//    }
//}