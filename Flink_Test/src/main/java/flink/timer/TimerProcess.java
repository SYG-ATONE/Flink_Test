package flink.timer;

import flink.sink.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class TimerProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * 需求：
         * 	监控水位传感器的水位值，如果水位值在五分钟之内(processing time)连续上升，则报警。
         */

        SingleOutputStreamOperator<WaterSensor> stream = env
                .socketTextStream("hadoop102", 9999)  // 在socket终端只输入秒级别的时间戳
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                });

        WatermarkStrategy<WaterSensor> wms = WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                //注：此处的recordTimestamp是指的返回的时间事件时间戳
//                .withTimestampAssigner((element, recordTimestamp) -> element.getTs() * 1000);
        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor waterSensor, long l) {
                return waterSensor.getTs()*1000;
            }
        });

        stream
                .assignTimestampsAndWatermarks(wms)
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    int lastVc = 0 ;
                    long timeTs = Long.MIN_VALUE;

                    @Override
                    public void processElement(WaterSensor waterSensor, Context context, Collector<String> collector) throws Exception {

                        if (waterSensor.getVc()>lastVc){
                            if (timeTs == Long.MIN_VALUE) {
                                System.out.println("注册。。。。");
                                timeTs = context.timestamp()+5000L ;
                                context.timerService().registerEventTimeTimer(timeTs);
                            }
                        }
                        else {
                            context.timerService().deleteEventTimeTimer(timeTs);
                            timeTs = Long.MIN_VALUE ;
                        }
                        lastVc = waterSensor.getVc() ;
                        System.out.println(lastVc);
                    }
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + " 报警!!!!");
                        timeTs = Long.MIN_VALUE;
                    }

                })
                .print() ;

    }
}
