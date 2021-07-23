//package flink.etl;
//
//
//import bigdata.realtime.common.sink.ClickHouseSink;
//import bigdata.realtime.warehouse.utils.*;
//import com.alibaba.fastjson.JSONArray;
//import com.alibaba.fastjson.JSONObject;
//import com.google.common.collect.Lists;
//import org.apache.flink.api.common.functions.RichFlatMapFunction;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.common.state.BroadcastState;
//import org.apache.flink.api.common.state.MapStateDescriptor;
//import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.metrics.Counter;
//import org.apache.flink.streaming.api.datastream.*;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
//import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
//import org.apache.flink.util.Collector;
//import org.joda.time.DateTime;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.sql.*;
//import java.util.*;
//
///**
// * 一起学日志ETL 实时同步进clickhouse
// *
// * @author <a href="mailto:tianyu.zhang@17zuoye.com">tianyu.zhang</a>
// * @date 2021/01/28 11:30.
// */
//
//public class BdmParentLogsPointETL {
//    private static Logger logger = LoggerFactory.getLogger(BdmParentLogsPointETL.class);
//    public static void main(String[] args) throws Exception{
//
//
//        String topicName = "vox_logs.parents_product";
//        String bootStrapServer = null;
//        String zookeeper = "";
//        String groupId = "realtime_vox_logs.tianyu_bdm_parent_produce_logs_point_ETLss232";
//
//        try {
//            bootStrapServer = PropertiesUtil.getProperty("kafka.online.bootstrap.servers");
////            zookeeper = PropertiesUtil.getProperty("kafka.online.zookeeper");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        Properties property = KafkaConfig10.getSpecifyKafkaConsumerProperty(groupId, bootStrapServer, zookeeper, "latest");
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        SingleOutputStreamOperator<Map<String,String>> chainStreamSource = env.addSource(new ChainFromHive());
//        chainStreamSource.name("chainStreamSource");
//        chainStreamSource.setParallelism(1);
//
//        MapStateDescriptor<String,Map<String, String>> configFilter = new MapStateDescriptor<>(
//                "chainFromHive",
//                Types.STRING,
//                Types.MAP(Types.STRING,Types.STRING));
//
//        BroadcastStream<Map<String, String>> broadcastConfig = chainStreamSource.broadcast(configFilter);
//
//
//        DataStreamSource<String> kafkaSource = env.addSource(new FlinkKafkaConsumer010<>(topicName, new SimpleStringSchema(), property));
//        kafkaSource.name("kafkaSource");
//        kafkaSource.setParallelism(10);
//        kafkaSource.print();
//
//        env.execute();
//
//
////        String a="{\"_l\":\"alert\",\"_type\":\"2\",\"android_id\":\"66e3672d6287cc8f\",\"channel\":\"202001\",\"client_ip\":\"183.219.255.28\",\"cpu\":\"arm64-v8a/8/300000/1708800\",\"current_sid\":\"3154670855\",\"forwarded_ip\":\"183.219.255.28\",\"memory\":\"1880/5621\",\"model\":\"PBET00\",\"native_version\":\"3.4.12.1019\",\"native_version_code\":766,\"network\":\"wifi\",\"os\":\"android\",\"sa\":\"[{\\\"_track_id\\\":2144640666,\\\"time\\\":1612111345153,\\\"type\\\":\\\"track\\\",\\\"distinct_id\\\":\\\"276228534\\\",\\\"lib\\\":{\\\"$lib\\\":\\\"Android\\\",\\\"$lib_version\\\":\\\"3.2.0\\\",\\\"$app_version\\\":\\\"3.4.12.1019\\\",\\\"spu_id\\\":\\\"202009070001\\\",\\\"$lib_method\\\":\\\"code\\\",\\\"$lib_detail\\\":\\\"com.sensorsdata.analytics.android.sdk.SensorsDataAPI##trackEvent##SensorsDataAPI.java##3087\\\"},\\\"event\\\":\\\"Parent_firstModule_Load\\\",\\\"properties\\\":{\\\"$lib\\\":\\\"Android\\\",\\\"spu_id\\\":\\\"202007310001\\\",\\\"$os_version\\\":\\\"10\\\",\\\"$device_id\\\":\\\"66e3672d6287cc8f\\\",\\\"spu_id\\\":\\\"202009070001\\\",\\\"$lib_version\\\":\\\"3.2.0\\\",\\\"$model\\\":\\\"PBET00\\\",\\\"$os\\\":\\\"Android\\\",\\\"$screen_width\\\":1080,\\\"$screen_height\\\":2340,\\\"$manufacturer\\\":\\\"OPPO\\\",\\\"$app_version\\\":\\\"3.4.12.1019\\\",\\\"uuid\\\":\\\"ca1caf11-900f-4fa1-8724-a462710eba4a\\\",\\\"current_sid\\\":\\\"3154670855\\\",\\\"pid\\\":\\\"276228534\\\",\\\"platform\\\":\\\"一起学\\\",\\\"business\\\":\\\"一起学\\\",\\\"app_type\\\":\\\"production\\\",\\\"channel\\\":\\\"202001\\\",\\\"session_id\\\":\\\"1612111336585\\\",\\\"android_id\\\":\\\"66e3672d6287cc8f\\\",\\\"mtype\\\":\\\"gphone\\\",\\\"gyro\\\":\\\"{\\\\\\\"x\\\\\\\":0.05697076767683029,\\\\\\\"y\\\\\\\":0.06642390042543411,\\\\\\\"z\\\\\\\":0.014385865069925785}\\\",\\\"is_root\\\":false,\\\"vpn\\\":false,\\\"_bd\\\":\\\"3-1-0-1217\\\",\\\"$wifi\\\":true,\\\"$network_type\\\":\\\"WIFI\\\",\\\"firstModuleName\\\":\\\"网校\\\",\\\"$is_first_day\\\":false},\\\"_flush_time\\\":1612111345307},{\\\"_track_id\\\":-409727412,\\\"time\\\":1612111345172,\\\"type\\\":\\\"track\\\",\\\"distinct_id\\\":\\\"276228534\\\",\\\"lib\\\":{\\\"$lib\\\":\\\"Android\\\",\\\"$lib_version\\\":\\\"3.2.0\\\",\\\"$app_version\\\":\\\"3.4.12.1019\\\",\\\"$lib_method\\\":\\\"code\\\",\\\"$lib_detail\\\":\\\"com.sensorsdata.analytics.android.sdk.SensorsDataAPI##trackEvent##SensorsDataAPI.java##3087\\\"},\\\"event\\\":\\\"Parent_firstModule_Load\\\",\\\"properties\\\":{\\\"$lib\\\":\\\"Android\\\",\\\"$os_version\\\":\\\"10\\\",\\\"$device_id\\\":\\\"66e3672d6287cc8f\\\",\\\"$lib_version\\\":\\\"3.2.0\\\",\\\"$model\\\":\\\"PBET00\\\",\\\"$os\\\":\\\"Android\\\",\\\"$screen_width\\\":1080,\\\"$screen_height\\\":2340,\\\"$manufacturer\\\":\\\"OPPO\\\",\\\"$app_version\\\":\\\"3.4.12.1019\\\",\\\"uuid\\\":\\\"ca1caf11-900f-4fa1-8724-a462710eba4a\\\",\\\"current_sid\\\":\\\"3154670855\\\",\\\"pid\\\":\\\"276228534\\\",\\\"platform\\\":\\\"一起学\\\",\\\"business\\\":\\\"一起学\\\",\\\"app_type\\\":\\\"production\\\",\\\"channel\\\":\\\"202001\\\",\\\"session_id\\\":\\\"1612111336585\\\",\\\"android_id\\\":\\\"66e3672d6287cc8f\\\",\\\"mtype\\\":\\\"gphone\\\",\\\"gyro\\\":\\\"{\\\\\\\"x\\\\\\\":0.05697076767683029,\\\\\\\"y\\\\\\\":0.06642390042543411,\\\\\\\"z\\\\\\\":0.014385865069925785}\\\",\\\"is_root\\\":false,\\\"vpn\\\":false,\\\"_bd\\\":\\\"3-1-0-1217\\\",\\\"$wifi\\\":true,\\\"$network_type\\\":\\\"WIFI\\\",\\\"firstModuleName\\\":\\\"学习\\\",\\\"$is_first_day\\\":false},\\\"_flush_time\\\":1612111345307},{\\\"_track_id\\\":1958064136,\\\"time\\\":1612111345183,\\\"type\\\":\\\"track\\\",\\\"distinct_id\\\":\\\"276228534\\\",\\\"lib\\\":{\\\"$lib\\\":\\\"Android\\\",\\\"$lib_version\\\":\\\"3.2.0\\\",\\\"$app_version\\\":\\\"3.4.12.1019\\\",\\\"$lib_method\\\":\\\"code\\\",\\\"$lib_detail\\\":\\\"com.sensorsdata.analytics.android.sdk.SensorsDataAPI##trackEvent##SensorsDataAPI.java##3087\\\"},\\\"event\\\":\\\"Parent_firstModule_Load\\\",\\\"properties\\\":{\\\"$lib\\\":\\\"Android\\\",\\\"$os_version\\\":\\\"10\\\",\\\"spu_id\\\":\\\"202009070001\\\",\\\"$device_id\\\":\\\"66e3672d6287cc8f\\\",\\\"$lib_version\\\":\\\"3.2.0\\\",\\\"$model\\\":\\\"PBET00\\\",\\\"$os\\\":\\\"Android\\\",\\\"$screen_width\\\":1080,\\\"$screen_height\\\":2340,\\\"$manufacturer\\\":\\\"OPPO\\\",\\\"$app_version\\\":\\\"3.4.12.1019\\\",\\\"uuid\\\":\\\"ca1caf11-900f-4fa1-8724-a462710eba4a\\\",\\\"current_sid\\\":\\\"3154670855\\\",\\\"pid\\\":\\\"276228534\\\",\\\"platform\\\":\\\"一起学\\\",\\\"business\\\":\\\"一起学\\\",\\\"app_type\\\":\\\"production\\\",\\\"channel\\\":\\\"202001\\\",\\\"session_id\\\":\\\"1612111336585\\\",\\\"android_id\\\":\\\"66e3672d6287cc8f\\\",\\\"mtype\\\":\\\"gphone\\\",\\\"gyro\\\":\\\"{\\\\\\\"x\\\\\\\":0.05697076767683029,\\\\\\\"y\\\\\\\":0.06642390042543411,\\\\\\\"z\\\\\\\":0.014385865069925785}\\\",\\\"is_root\\\":false,\\\"vpn\\\":false,\\\"_bd\\\":\\\"3-1-0-1217\\\",\\\"$wifi\\\":true,\\\"$network_type\\\":\\\"WIFI\\\",\\\"firstModuleName\\\":\\\"家长\\\",\\\"$is_first_day\\\":false},\\\"_flush_time\\\":1612111345307},{\\\"_track_id\\\":1550927516,\\\"time\\\":1612111345195,\\\"type\\\":\\\"track\\\",\\\"distinct_id\\\":\\\"276228534\\\",\\\"lib\\\":{\\\"$lib\\\":\\\"Android\\\",\\\"$lib_version\\\":\\\"3.2.0\\\",\\\"$app_version\\\":\\\"3.4.12.1019\\\",\\\"$lib_method\\\":\\\"code\\\",\\\"$lib_detail\\\":\\\"com.sensorsdata.analytics.android.sdk.SensorsDataAPI##trackEvent##SensorsDataAPI.java##3087\\\"},\\\"event\\\":\\\"Parent_firstModule_Load\\\",\\\"sputies\\\":{\\\"$lib\\\":\\\"Android\\\",\\\"$os_version\\\":\\\"10\\\",\\\"$device_id\\\":\\\"66e3672d6287cc8f\\\",\\\"$lib_version\\\":\\\"3.2.0\\\",\\\"$model\\\":\\\"PBET00\\\",\\\"$os\\\":\\\"Android\\\",\\\"$screen_width\\\":1080,\\\"$screen_height\\\":2340,\\\"$manufacturer\\\":\\\"OPPO\\\",\\\"$app_version\\\":\\\"3.4.12.1019\\\",\\\"uuid\\\":\\\"ca1caf11-900f-4fa1-8724-a462710eba4a\\\",\\\"current_sid\\\":\\\"3154670855\\\",\\\"pid\\\":\\\"276228534\\\",\\\"platform\\\":\\\"一起学\\\",\\\"business\\\":\\\"一起学\\\",\\\"app_type\\\":\\\"production\\\",\\\"channel\\\":\\\"202001\\\",\\\"session_id\\\":\\\"1612111336585\\\",\\\"android_id\\\":\\\"66e3672d6287cc8f\\\",\\\"mtype\\\":\\\"gphone\\\",\\\"gyro\\\":\\\"{\\\\\\\"x\\\\\\\":0.05697076767683029,\\\\\\\"y\\\\\\\":0.06642390042543411,\\\\\\\"z\\\\\\\":0.014385865069925785}\\\",\\\"is_root\\\":false,\\\"vpn\\\":false,\\\"_bd\\\":\\\"3-1-0-1217\\\",\\\"$wifi\\\":true,\\\"$network_type\\\":\\\"WIFI\\\",\\\"firstModuleName\\\":\\\"我的\\\",\\\"$is_first_day\\\":false},\\\"_flush_time\\\":1612111345307}]\",\"screen\":\"1080/2132\",\"server_type\":\"prod\",\"session_id\":\"1612111336585\",\"system_version\":\"10\",\"time\":\"2021-02-01 00:42:25:310\",\"use_proxy\":true,\"userid\":\"276228534\",\"uuid\":\"ca1caf11-900f-4fa1-8724-a462710eba4a\",\"wifi_info\":\"{}\"}";
////        String a="{"corpId":"wwb9b0fe0fb4c7f023","changeEvent":"create","sendTime":"","source":"","data":{"chatId":"fake_data","name":"fake_data","owner":"fake_data","createTime":"2024-12-15 09:55:44","notice":"fake_data","corpId":"fake_data","memberTotal":28,"outGroupTotal":31,"protect":true,"createDatetime":"2025-02-14 00:59:36","updateDatetime":"2015-06-30 13:52:25","operator":"","tagList":[{"grouupId":"60c05e2a3e32c6771395e30c","groupName":"测试","type":"EDUCATION_SERVICE_TAG","tagId":"c5e870d78af14f3ba660272433ea547c","tagName":"测试"}]}}";
//
////        DataStreamSource<String> kafkaSource = env.fromCollection(Lists.newArrayList(a));
//
////        String a="{\"_track_id\":-824222425,\"lib\":{\"$lib\":\"js\",\"$lib_method\":\"code\",\"$lib_version\":\"1.12.18\",\"$app_version\":\"3.4.12.1019\"},\"distinct_id\":\"269595093\",\"_flush_time\":1612128439990,\"time\":1612128426292,\"type\":\"track\",\"event\":\"$pageview\",\"properties\":{\"app_type\":\"production\",\"$os\":\"Android\",\"channel\":\"202001\",\"$is_first_time\":true,\"pid\":\"269595093\",\"_posid\":\"1-12-61\",\"$wifi\":true,\"$network_type\":\"WIFI\",\"$screen_height\":2340,\"is_root\":false,\"$referrer\":\"\",\"uuid\":\"7dc30022-15df-4eda-b666-6264baabd442\",\"platform\":\"一起学\",\"mtype\":\"gphone\",\"$url_path\":\"/karp/report/index/analysis\",\"current_sid\":\"3147623470\",\"$device_id\":\"d16f4cfbbd1523b5\",\"$latest_search_keyword\":\"未取到值_直接打开\",\"$url\":\"https://parent.17zuoye.com/karp/report/index/analysis?navBarIsShow=false&rel=native&useNewCore=wk&_posid=1-12-61&app_version=3.4.12.1019&client_type=mobile&client_name=17Parent&env=prod&imei=7dc30022-15df-4eda-b666-6264baabd442&model=V1965A&system_version=10&session_key=1181c2ac5cc201462fbf696fd4949065&app_product_id=200&channel=202001&sid=3147623470\",\"$latest_referrer\":\"\",\"href\":\"https://parent.17zuoye.com/karp/report/index/analysis?navBarIsShow=false&rel=native&useNewCore=wk&_posid=1-12-61&app_version=3.4.12.1019&client_type=mobile&client_name=17Parent&env=prod&imei=7dc30022-15df-4eda-b666-6264baabd442&model=V1965A&system_version=10&session_key=1181c2ac5cc201462fbf696fd4949065&app_product_id=200&channel=202001&sid=3147623470\",\"gyro\":\"{\\\"x\\\":-0.24350999295711517,\\\"y\\\":-0.02348623052239418,\\\"z\\\":-0.07283201068639755}\",\"_bd\":\"3-1-0-1270\",\"is_app\":true,\"$os_version\":\"10\",\"business\":\"一起学\",\"$referrer_host\":\"\",\"$is_first_day\":true,\"$model\":\"V1965A\",\"$screen_width\":1080,\"session_id\":\"1612128394491\",\"userAgent\":\"Mozilla/5.0 (Linux; Android 10; V1965A Build/QP1A.190711.020; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/78.0.3904.96 Mobile Safari/537.36 17Parent android 3.4.12.1019 17Parent/3.4.12.1019\",\"$app_version\":\"3.4.12.1019\",\"$lib\":\"js\",\"referrer\":\"\",\"grade_level\":\"五年级\",\"vpn\":false,\"$title\":\"\u200E\",\"$lib_version\":\"1.12.18\",\"$latest_traffic_source_type\":\"直接流量\",\"school_level\":\"PRIMARY_SCHOOL\",\"$latest_referrer_host\":\"\",\"android_id\":\"d16f4cfbbd1523b5\",\"$manufacturer\":\"vivo\"}}";
////        DataStreamSource<String> kafkaSource = env.fromCollection(Lists.newArrayList(a));
//
//
//        SingleOutputStreamOperator<List<Object>> kafkaParseStream = kafkaSource.flatMap(new ParserFlatMap());
//        kafkaParseStream.name("parse");
//        kafkaParseStream.setParallelism(20);
//
//
//
//        BroadcastConnectedStream<List<Object>, Map<String, String>> connectedStream = kafkaParseStream.connect(broadcastConfig);
//        SingleOutputStreamOperator<List<Object>> result = connectedStream.process(new BroadcastProcessFunction<List<Object>, Map<String, String>, List<Object>>() {
//
//            @Override
//            //更新广播状态
//            public void processBroadcastElement(Map<String, String> chainData, Context context, Collector<List<Object>> collector) throws Exception {
//
//                BroadcastState<String, Map<String, String>> broadcastState = context.getBroadcastState(configFilter);
//                broadcastState.clear();
//                broadcastState.put("tian", chainData);
//
//
//            }
//
//            @Override
//            public void processElement(List<Object> objects, ReadOnlyContext readOnlyContext, Collector<List<Object>> collector) throws Exception {
//
//                ReadOnlyBroadcastState<String, Map<String, String>> broadcastState = readOnlyContext.getBroadcastState(configFilter);
//                Map<String, String> broadChainData = broadcastState.get("tian");
//                if (broadChainData == null) {
//                    Thread.sleep(100);
//                    broadChainData = broadcastState.get("tian");
//                }
//                if (broadChainData != null) {
//                    Object s = objects.get(37);
//                    String spu_id_new = s.toString();
//                    String spuType = broadChainData.get(spu_id_new);
//                    if (spuType == null) {
//                        spuType = "";
//                    } else if (spuType.equals("LITE_COURSE")) {
//                        spuType = "轻课";
//                    } else if (spuType.equals("TRAINING")) {
//                        spuType = "训练营";
//                    }
//                    objects.add(38, spuType);
//                    collector.collect(objects);
//                }
//            }
//
//        });
//        result.setParallelism(8);
//
//
//
//
//
//        DataStreamSink<List<Object>> sink = result.addSink(new Result());
//        sink.name("clickhouseSink");
//        sink.setParallelism(1);
//
//        try {
//            env.execute(BdmParentLogsPointETL.class.getName());
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//
//
////        HiveUtil util=null;
////        String url = PropertiesUtil.getProperty("hive.jdbc.url");
////        String name=PropertiesUtil.getProperty("hive.jdbc.username");
////        String password=PropertiesUtil.getProperty("hive.jdbc.password");
////        util = new HiveUtil(url, "fdm", name, password);
////        util.getConnection();
////        List<String> list = util.executeQuery("select id,sputype from fdm.fdm_mongo_vst_study_course_arthur_spu_chain where dp='ACTIVE'");
////        Map<String, String> result = new HashMap<>();
////        for (int i=0;i<list.size();i++){
////            String s = list.get(i);
////            String id=s.split("\t")[0];
////            String sputype=s.split("\t")[1];
////            result.put(id,sputype);
////        }
////
////        System.out.println(result);
//
//
//    }
//    public static class ParserFlatMap extends RichFlatMapFunction<String, List<Object>> {
//
//        //原始日志
//        Counter originLog;
//        //解析成功的原始日志
//        Counter originSuccLog;
//        //解析后的单条日志
//        Counter singleLog;
//        //解析后成功的单条日志
//        Counter singleSuccLog;
//
//        @Override
//        public void open(Configuration parameters) throws Exception {
//            super.open(parameters);
//            originLog = getRuntimeContext()
//                    .getMetricGroup()
//                    .counter("bdm_parent_log_point:origin_log");
//            originSuccLog = getRuntimeContext()
//                    .getMetricGroup()
//                    .counter("bdm_parent_log_point:origin_succ_log");
//            singleLog = getRuntimeContext()
//                    .getMetricGroup()
//                    .counter("bdm_parent_log_point:single_log");
//            singleSuccLog = getRuntimeContext()
//                    .getMetricGroup()
//                    .counter("bdm_parent_log_point:single_succ_log");
//        }
//
//        @Override
//        public void flatMap(String value, Collector<List<Object>> out) throws Exception {
//            originLog.inc();
//            try{
//                JSONObject jsonObject = JSONObject.parseObject(value);
//                String sa = jsonObject.getString("sa");
//                if (sa==null){
//                    return;
//                }
//                JSONArray jsonArray = JSONArray.parseArray(sa);
//                if (jsonArray == null) {
//                    return;
//                }
//
//                Iterator<Object> iterator = jsonArray.iterator();
//                if (iterator == null) {
//                    return;
//                }
//
//                DateTime now = new DateTime();
//
//                while (iterator.hasNext()){
//                    try{
//                        String item = iterator.next().toString();
//                        JSONObject inSaJson = JSONObject.parseObject(item);
//                        Long time = inSaJson.getLong("time");
//                        if (time == null) {
//                            continue;
//                        }
//
//                        boolean b = checkDataTimeRange(time, now);
//                        if (!b) {
//                            continue;
//                        }
//
//                        String event = inSaJson.getString("event").replaceAll("\\t", "").replaceAll("\\n", "").replaceAll("\\r", "");;
//                        if (event == null) {
//                            continue;
//                        }
//
//                        String propertiesJsonString = inSaJson.getString("properties");
//                        JSONObject inPropertiesJson = JSONObject.parseObject(propertiesJsonString);
//
//                        String project = "parents_product";
//                        String pid = inPropertiesJson.getString("pid") == null ? "" : inPropertiesJson.getString("pid");
//                        String platform = inPropertiesJson.getString("platform") == null ? "" : inPropertiesJson.getString("platform");
//                        String ip = inPropertiesJson.getString("$ip") == null ? "" : inPropertiesJson.getString("$ip");
//                        String lib_version = inPropertiesJson.getString("$lib_version") == null ? "" : inPropertiesJson.getString("$lib_version");
//                        String lib = inPropertiesJson.getString("$lib") == null ? "" : inPropertiesJson.getString("$lib");
//                        String country = inPropertiesJson.getString("$country") == null ? "" : inPropertiesJson.getString("$country");
//
//                        Double screen_width1 = Math.ceil(inPropertiesJson.getFloat("$screen_width") == null ? 0 : inPropertiesJson.getFloat("$screen_width"));
//                        Long screen_width=screen_width1.longValue();
//
//                        Double screen_height1 = Math.ceil(inPropertiesJson.getFloat("$screen_height") == null ? 0 : inPropertiesJson.getFloat("$screen_height"));
//                        Long screen_height=screen_height1.longValue();
//
//                        String app_version = inPropertiesJson.getString("$app_version") == null ? "" : inPropertiesJson.getString("$app_version");
//                        String latest_search_keyword = inPropertiesJson.getString("$latest_search_keyword") == null ? "" : inPropertiesJson.getString("$latest_search_keyword").replaceAll("\\t", "").replaceAll("\\n", "").replaceAll("\\r", "");
//                        String os = inPropertiesJson.getString("$os") == null ? "" : inPropertiesJson.getString("$os");
//                        String os_version = inPropertiesJson.getString("$os_version") == null ? "" : inPropertiesJson.getString("$os_version");
//                        int wifi = inPropertiesJson.getBoolean("$wifi") == null ? 0 : inPropertiesJson.getBoolean("$wifi") == true ? 1 : 0;
//                        int is_login_id = inPropertiesJson.getBoolean("$is_login_id") == null ? 0 : inPropertiesJson.getBoolean("$is_login_id") == true ? 1 : 0;
//                        int is_first_day = inPropertiesJson.getBoolean("$is_first_day") == null ? 0 : inPropertiesJson.getBoolean("$is_first_day") == true ? 1 : 0;
//                        String latest_traffic_source_type = inPropertiesJson.getString("$latest_traffic_source_type") == null ? "" : inPropertiesJson.getString("$latest_traffic_source_type");
//                        String browser = inPropertiesJson.getString("$browser") == null ? "" : inPropertiesJson.getString("$browser");
//                        String browser_version = inPropertiesJson.getString("$browser_version") == null ? "" : inPropertiesJson.getString("$browser_version");
//                        String bot_name = inPropertiesJson.getString("$bot_name") == null ? "" : inPropertiesJson.getString("$bot_name");
//                        String province = inPropertiesJson.getString("$province") == null ? "" : inPropertiesJson.getString("$province");
//                        String city = inPropertiesJson.getString("$city") == null ? "" : inPropertiesJson.getString("$city");
//                        String latest_referrer = inPropertiesJson.getString("$latest_referrer") == null ? "" : inPropertiesJson.getString("$latest_referrer");
//                        String latest_referrer_host = inPropertiesJson.getString("$latest_referrer_host") == null ? "" : inPropertiesJson.getString("$latest_referrer_host");
//                        String network_type = inPropertiesJson.getString("$network_type") == null ? "" : inPropertiesJson.getString("$network_type");
//                        String device_id = inPropertiesJson.getString("$device_id") == null ? "" : inPropertiesJson.getString("$device_id");
//                        String manufacturer = inPropertiesJson.getString("$manufacturer") == null ? "" : inPropertiesJson.getString("$manufacturer");
//                        String model = inPropertiesJson.getString("$model") == null ? "" : inPropertiesJson.getString("$model");
//                        String carrier = inPropertiesJson.getString("$carrier") == null ? "" : inPropertiesJson.getString("$carrier");
//                        String business = inPropertiesJson.getString("business") == null ? "" : inPropertiesJson.getString("business");
//
//                        String sku_id = inPropertiesJson.getString("sku_id") == null ? (inPropertiesJson.getString("skuid") == null ? "" : inPropertiesJson.getString("skuid")) : inPropertiesJson.getString("sku_id");
//                        String bd = inPropertiesJson.getString("bd") == null ? "" : inPropertiesJson.getString("bd");
//
//                        String [] bdArray =bd.split("-");
//                        String bd_channel="";
//                        if(bdArray.length==3 || bdArray.length==4){
//                            bd_channel=bdArray[0]+"-"+bdArray[1]+"-"+bdArray[2];
//                        }else{
//                            bd_channel="";
//                        }
//                        String bd_activity="";
//                        if(bdArray.length==4){
//                            bd_activity=bdArray[3];
//                        }else{
//                            bd_activity="";
//                        }
//
//
//                        String spu_id_new="";
//                        if(inPropertiesJson.getString("spu_id") != null){
//                            spu_id_new=inPropertiesJson.getString("spu_id");
//                        }else if(inPropertiesJson.getString("spuid") != null){
//                            spu_id_new=inPropertiesJson.getString("spuid");
//                        }else if(inPropertiesJson.getString("sku_id") != null){
//                            spu_id_new=inPropertiesJson.getString("sku_id").substring(0,12);
//                        }else if(inPropertiesJson.getString("skuid") != null){
//                            spu_id_new=inPropertiesJson.getString("skuid").substring(0,12);
//                        }else{
//                            spu_id_new="";
//                        }
//
//                        DateTime dateTime = new DateTime(time);
//                        String dt = now.toString("yyyy-MM-dd");
//                        String dtt = now.toString("yyyy-MM-dd");
//                        String timeString = dateTime.toString("yyyy-MM-dd HH:mm:ss");
//                        String hour = now.toString("yyyy-MM-dd HH:mm:ss").substring(11,13);
//
//                        List<Object> objects = Lists.newArrayList();
//                        objects.add(dt);
//                        objects.add(timeString);
//                        objects.add(event);
//                        objects.add(project);
//                        objects.add(pid);
//                        objects.add(platform);
//                        objects.add(ip);
//                        objects.add(lib_version);
//                        objects.add(lib);
//                        objects.add(country);
//                        objects.add(screen_width);
//                        objects.add(screen_height);
//                        objects.add(app_version);
//                        objects.add(latest_search_keyword);
//                        objects.add(os);
//                        objects.add(os_version);
//                        objects.add(wifi);
//                        objects.add(is_login_id);
//                        objects.add(is_first_day);
//                        objects.add(latest_traffic_source_type);
//                        objects.add(browser);
//                        objects.add(browser_version);
//                        objects.add(bot_name);
//                        objects.add(province);
//                        objects.add(city);
//                        objects.add(latest_referrer);
//                        objects.add(latest_referrer_host);
//                        objects.add(network_type);
//                        objects.add(device_id);
//                        objects.add(manufacturer);
//                        objects.add(model);
//                        objects.add(carrier);
//                        objects.add(business);
//                        objects.add(propertiesJsonString);
//                        objects.add(dtt);
//                        objects.add(hour);
//                        objects.add(sku_id);
//                        objects.add(spu_id_new);
//                        //
//                        objects.add(bd);
//                        objects.add(bd_channel);
//                        objects.add(bd_activity);
//                        out.collect(objects);
//
//                    }catch (Exception e) {
////                        logger.error("parse error, {}", e);
//                    }
//
//                }
//
//            }catch (Exception e2) {
////                logger.error("unknow error: msg: {}, {}", value, e2.getMessage());
//            }
//            if (new DateTime().getSecondOfMinute() == 0) {
////                logger.info("BdmParentLogsPointETLMetrics: originLog: {}, originLogParseSuccess: {}, sa_log: {}, sa_log_single_success: {}", originLog.getCount(), originSuccLog.getCount(), singleLog.getCount(), singleSuccLog.getCount());
//            }
//
//        }
//    }
//
//    public static class Result extends ClickHouseSink<List<Object>> {
//
//        private java.sql.Connection connection;
//
//        private String clickHouseTableName = "event_topic_hourlytime_flink_replica";
//
//        private List<List<Object>> list;
//        private DateTime lastCommitTime;
//
//        private List<String> fields;
//
//        Counter clickHouseSucc;
//        Counter clickHouseRece;
//        Counter clickHouseFail;
//
//        @Override
//        public void open(Configuration parameters) throws Exception {
//            super.open(parameters);
//
////            String url = PropertiesUtil.getProperty("ziyanck.jdbc.url");
//            String url = "10.80.4.77:8123";
//            String um = PropertiesUtil.getProperty("ziyanck.jdbc.username");
//            String db = "parent";
//            String pd = PropertiesUtil.getProperty("ziyanck.jdbc.password");
//
//            connection = ClickHouseUtil.connect(url, db, um, pd);
//
//            list = Lists.newArrayList();
//            lastCommitTime = new DateTime();
//
//            fields = Lists.newArrayList();
//
//            fields.add("dt");
//            fields.add("time");
//            fields.add("event");
//            fields.add("project");
//            fields.add("pid");
//            fields.add("platform");
//            fields.add("ip");
//            fields.add("lib_version");
//            fields.add("lib");
//            fields.add("country");
//            fields.add("screen_width");
//            fields.add("screen_height");
//            fields.add("app_version");
//            fields.add("latest_search_keyword");
//            fields.add("os");
//            fields.add("os_version");
//            fields.add("wifi");
//            fields.add("is_login_id");
//            fields.add("is_first_day");
//            fields.add("latest_traffic_source_type");
//            fields.add("browser");
//            fields.add("browser_version");
//            fields.add("bot_name");
//            fields.add("province");
//            fields.add("city");
//            fields.add("latest_referrer");
//            fields.add("latest_referrer_host");
//            fields.add("network_type");
//            fields.add("device_id");
//            fields.add("manufacturer");
//            fields.add("model");
//            fields.add("carrier");
//            fields.add("business");
//            fields.add("extra_01");
//            fields.add("dtt");
//            fields.add("hour");
//            fields.add("sku_id");
//            fields.add("spu_id");
//            fields.add("spu_type");
//            fields.add("_bd");
//            fields.add("_bd_channel");
//            fields.add("_bd_activity");
//
//            clickHouseSucc = getRuntimeContext().getMetricGroup().counter("bdm_parent_log_point:clickhouse_succ");
//            clickHouseRece = getRuntimeContext().getMetricGroup().counter("bdm_parent_log_point:clickhouse_rece");
//            clickHouseFail = getRuntimeContext().getMetricGroup().counter("bdm_parent_log_point:clickhouse_fail");
//        }
//
//        @Override
//        public void close() throws Exception {
//            if (list.size() > 0) {
//                clickHouseSucc.inc(list.size());
//                try {
//                    try {
//                        ClickHouseUtil.insertData(connection, clickHouseTableName, list, fields);
//                    } catch (Exception e1) {
//                        logger.error("ziyanck close end, insert clickhouse exception {}", e1);
//                    }
//                } catch (Exception e) {
//                    logger.error("write to clickhouse exception, {}", e.getMessage());
//                    for (List<Object> objects : list) {
//                        logger.info("data_bakup: {}", JSONObject.toJSONString(objects, true));
//                    }
//                }
//            }
//
//            logger.info("BdmParentLogPointNewETLMetrics: clickHouse receive_num: {}, write_success: {}", clickHouseRece.getCount(), clickHouseSucc.getCount());
//            super.close();
//
//            ClickHouseUtil.closeConnection(connection);
//        }
//
//        @Override
//        public void invoke(List<Object> value, Context context) throws Exception {
//
//            clickHouseRece.inc();
//
//            list.add(value);
//
//            DateTime d = new DateTime();
//            if (list.size() > 200000 || d.getMillis() - lastCommitTime.getMillis() > 5 * 1000) {
//                try {
//                    long start = System.currentTimeMillis();
//                    ClickHouseUtil.insertData(connection, clickHouseTableName, list, fields);
//                    long duration = System.currentTimeMillis() - start;
//                    logger.info("ziyanck batch duration {}, dataSize: {}", duration, list.size());
//                    clickHouseSucc.inc(list.size());
//                } catch (Exception e1) {
//                    logger.error("ziyanck close end, insert clickhouse {}", e1);
//                    clickHouseFail.inc(list.size());
//                }
//
//                logger.info("ziyanck BdmParentLogPointNewETLMetrics: clickHouse receive_num: {}, write_success: {}, write_error: {}", clickHouseRece.getCount(), clickHouseSucc.getCount(), clickHouseFail.getCount());
//
//                list.clear();
//                lastCommitTime = d;
//            }
//        }
//    }
//
//
//
//    /**
//     * 处理脏数据
//     *
//     * @param timestamp
//     * @param now
//     * @return
//     */
//    public static boolean checkDataTimeRange(long timestamp, DateTime now) {
//        //修改逻辑,时间最大值变更为 当前时间戳+1h
//        //long max = zeroTimeStamp + b;
//        long max = now.getMillis() + 3600 * 1000;
//        long min = now.minusDays(10).getMillis();
//        if (timestamp >= max || timestamp < min) {
//            return false;
//        }
//        return true;
//    }
//
//
//    public static class ChainFromHive extends RichSourceFunction<Map<String,String>>{
//        private Connection con =null;
//        private boolean flag = true;
//        private PreparedStatement statement=null;
//        private ResultSet resultSet=null;
//        private HiveUtil util=null;
//
//
//        @Override
//        public void run(SourceContext<Map<String,String>> sourceContext) throws Exception {
////            try {
////                while (flag) {
////                    resultSet = statement.executeQuery();
////                    Map<String, String> result = new HashMap<>();
////                    while (resultSet.next()) {
////                        String id = resultSet.getString("id");
////                        String sputype = resultSet.getString("sputype");
////                        result.put(id, sputype);
////                    }
////                    sourceContext.collect(result);
////                    Thread.sleep(43200000);
////                }
////            }catch (SQLException e){
////                throw new Exception(e);
////            }
////            List<String> list = util.executeQuery("select id,sputype from fdm.fdm_mongo_vst_study_course_arthur_spu_chain where dp='ACTIVE'");
////            Map<String, String> result = new HashMap<>();
////            for (int i=0;i<list.size();i++){
////                String s = list.get(i);
////                String id=s.split("\t")[0];
////                String sputype=s.split("\t")[1];
////                result.put(id,sputype);
////            }
////            sourceContext.collect(result);
////            Thread.sleep(5000);
//            while(flag==true) {
//                List<String> list = MysqlUtil.executeQuery(con, "select spuid,sputype from parent_logs_point_chain");
//                Map<String, String> result = new HashMap<>();
//                for (int i = 0; i < list.size(); i++) {
//                    String s = list.get(i);
//                    String id = s.split("\t")[0];
//                    String sputype = s.split("\t")[1];
//                    result.put(id, sputype);
//                }
//                sourceContext.collect(result);
//                MysqlUtil.closeConnection(con);
//                Thread.sleep(14400000);
//                con = MysqlUtil.getConnection("jdbc:mysql://10.200.2.228:5009/HS_Athena_Warehouse", "bigdata_test_rw", "CRonEHUXB32fMmac");
//
//            }
//        }
//
//        @Override
//        public void cancel() {
//            flag=false;
//        }
//
//        @Override
//        public void open(Configuration parameters) throws Exception {
//            super.open(parameters);
////            String url = PropertiesUtil.getProperty("hive.jdbc.url");
////            String name=PropertiesUtil.getProperty("hive.jdbc.username");
////            String password=PropertiesUtil.getProperty("hive.jdbc.password");
////            util = new HiveUtil(url, "fdm", name, password);
////            util.getConnection();
//
////            try {
////
////                Class.forName("org.apache.hive.jdbc.HiveDriver");
////                String url = PropertiesUtil.getProperty("hive.jdbc.url");
////                String name=PropertiesUtil.getProperty("hive.jdbc.username");
////                String password=PropertiesUtil.getProperty("hive.jdbc.password");
////                con = DriverManager.getConnection("jdbc:hive2://" + url + "/fdm", name, password);
////                String sql = "select id,sputype from fdm.fdm_mongo_vst_study_course_arthur_spu_chain where dp='ACTIVE'";
////                statement = con.prepareStatement(sql);
////            }catch (ClassNotFoundException e){
////                throw new Exception(e);
////            } catch (SQLException e) {
////                throw new Exception(e);
////            }
////            dw-17zuoye
//            con = MysqlUtil.getConnection("jdbc:mysql://10.200.2.228:5009/HS_Athena_Warehouse", "bigdata_test_rw", "CRonEHUXB32fMmac");
//
//
//        }
//
//
//        @Override
//        public void close() throws Exception {
////            try {
////                super.close();
////                if (con != null) con.close();
////                if (statement != null) statement.close();
////                if (resultSet != null) resultSet.close();
////            }catch (SQLException e){
////                throw e;
////            }
//            MysqlUtil.closeConnection(con);
//
//
//        }
//    }
//
//}
//
