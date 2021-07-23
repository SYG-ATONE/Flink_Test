package project_test1.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import project_test1.gmall.realtime.utils.MyKafkaUtil;

import java.text.SimpleDateFormat;
import java.util.Date;

public class BaseLogApp {
    //定义用户行为主题信息
    private static final String TOPIC_START = "dwd_start_log" ;

    private static final String TOPIC_PAGE = "dwd_page_log" ;

    private static final String TOPIC_DISPLAY = "dwd_display_log" ;

    public static void main(String[] args) throws Exception {
        //TODO 0.基本环境准备
       //创建Flink 流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度 这里和kafka分区数保持一致
        env.setParallelism(6) ;

        //设置CK相关的参数
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE) ;
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://mycluster/gmall/flink/checkpoint")) ;

//        System.setProperty("HADOOP_USER_NAME","atone") ;


        //指定消费者配置信息
        String groupID = "ods_dwd_base_log_app" ;
        String topic = "ods_base_log" ;

        //TODO 1.从kafka中读取数据
        //调用kafka工具类，从指定的kafka主题中读取数据
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupID);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //转换为json对象
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        return jsonObject;
                    }
                }
        );

        //打印测试
        jsonObjectDS.print() ;

        //TODO 2.识别新老用户
        //按照mid进行分组
        KeyedStream<JSONObject, String> midKeyedDS = jsonObjectDS.keyBy(
                data -> data.getJSONObject("common").getString("mid"));

        /**
         *  保存每个mid的首次访问日期，每条进入该算子的访问记录，都会把mid对应的首次访问时间读取出来，
         *  跟当前日期进行比较，只有首次访问时间不为空，且首次访问时间早于当日的，则认为该访客是老访客，否则是新访客。
         *  同时如果是新访客且没有访问记录的话，会写入首次访问时间。
         */
        //校验采集到的数据是新老访客
        SingleOutputStreamOperator<JSONObject> midWithNewFlagDS = midKeyedDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    //声明第一次访问日期的状态
                    private ValueState<String> firstVisitDateState;
                    //声明日期格式化对象
                    private SimpleDateFormat simpleDateFormat;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //初始化数据
                        firstVisitDateState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("newMidDateState", String.class)
                        );
                        simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObj) throws Exception {
                        //打印数据
                        System.out.println(jsonObj);

                        //获取访问标记  0代表老访客  1代表新访客
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");
                        //获取数据中的时间戳
                        Long ts = jsonObj.getLong("ts");

                        //判断标记如果为1，则继续校验数据
                        if ("1".equals(isNew)) {
                            //获取新访客的状态
                            String newMidDate = firstVisitDateState.value();
                            //获取当前数据日期
                            String tsDate = simpleDateFormat.format(new Date(ts));

                            //如果状态不为空，则说明该状态已经访问过 则将访问标记为 0
                            if (newMidDate != null && newMidDate.length() != 0) {
                                isNew = "0";
                                jsonObj.getJSONObject("common").put("is_new", isNew);
                            } else {
                                //通过上面复检后，该设备确实没有访问过，那么更新状态为当前日期
                                firstVisitDateState.update(tsDate);
                            }
                        }

                        return jsonObj;
                    }
                }
        );

        //打印测试
        midWithNewFlagDS.print() ;


        //TODO 3.利用侧输出流实现数据拆分
        /**
         * 根据日志数据内容,将日志数据分为3类, 页面日志、启动日志和曝光日志。
         * 页面日志输出到主流,启动日志输出到启动侧输出流,曝光日志输出到曝光日志侧输出流
         */

        //定义启动和曝光数据测侧输出流标签
        OutputTag<String> startTag = new OutputTag<>("start");
        OutputTag<String> displayTag = new OutputTag<>("display");

        //日志主要分页面和启动    页面日志中包含：事件|曝光|错误|公共
        //将不同的日志输出到不同的流中  页面日志输出到主流，启动日志输出到启动侧输出流，曝光日志输出到曝光日志测输出流

        SingleOutputStreamOperator<String> pageDStream = midWithNewFlagDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<String> out) throws Exception {

                        //获取数据中的启动相关字段
                        JSONObject startJsonObj = jsonObj.getJSONObject("start");
                        //将数据转换为字符串，准备向流中输出
                        String dataStr = jsonObj.toString();

                        //如果是启动日志，就输出到侧输出流
                        if (startJsonObj != null && startJsonObj.size() > 0) {
                            ctx.output(startTag, dataStr);
                        } else {
                            //非启动日志，则为页面日志或者曝光日志（携带页面信息）
                            System.out.println("PageString" + dataStr);
                            //将页面日志输出到主流中
                            out.collect(dataStr);
                            //获取数据中的曝光数据，如果不为空，则将每条曝光数据取出到曝光日志侧输出流
                            JSONArray displays = jsonObj.getJSONArray("displays");
                            if (displays != null && displays.size() > 0) {
                                for (int i = 0; i < displays.size(); i++) {
                                    JSONObject displayJsonObj = displays.getJSONObject(i);
                                    //获取页面id
                                    String pageId = jsonObj.getJSONObject("page").getString("page_id");
                                    //给每条曝光信息加上pageId
                                    displayJsonObj.put("page_id", pageId);
                                    //将曝光日志输出到测输出流中
                                    ctx.output(displayTag, displayJsonObj.toString());
                                }
                            }

                        }
                    }
                }
        );

        //获取侧输出流
        DataStream<String> startDStream = pageDStream.getSideOutput(startTag);
        DataStream<String> displayDStream = pageDStream.getSideOutput(displayTag);

        //打印测试
        pageDStream.print("page");
        startDStream.print() ;
        displayDStream.print() ;



        //TODO 4.将数据输出到kafka不同的主题中
        FlinkKafkaProducer<String> startSink = MyKafkaUtil.getKafkaSink(TOPIC_START);
        FlinkKafkaProducer<String> pageSink = MyKafkaUtil.getKafkaSink(TOPIC_PAGE);
        FlinkKafkaProducer<String> displaySink = MyKafkaUtil.getKafkaSink(TOPIC_DISPLAY);

        startDStream.addSink(startSink) ;
        pageDStream.addSink(pageSink) ;
        displayDStream.addSink(displaySink) ;


        //执行
        env.execute("dwd_base_log job") ;


    }
}
