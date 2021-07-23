package flink.state;

import flink.sink.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;


public class Flink02_State_Keyed_Value {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(2);
        ArrayList<WaterSensor> waterSensors = new ArrayList<WaterSensor>() ;

        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 40));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 30));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 15));

//        env.fromElements(waterSensors)
//                .map(new MapFunction<WaterSensor, WaterSensor>() {
//
//                    @Override
//                    public WaterSensor map(WaterSensor waterSensor) throws Exception {
//                           return new WaterSensor (
//                                   waterSensor.getId() ,
//                                   Long.valueOf(waterSensor.getTs()) ,
//                                   waterSensor.getVc()
//                           );
//
//                    }
//                })


        env
                .readTextFile("input/waterSensors.txt")
                .map( line -> {
                    String[] split = line.split(",");
                    return new WaterSensor(
                            split[0] ,
                            Long.valueOf(split[1]) ,
                            Integer.valueOf(split[2])
                    );
                        }
                )
                .keyBy(x -> x.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                     private ValueState<Integer> state;
                     @Override
                     public void open(Configuration parameters) throws Exception {
                     state = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("state", Integer.class));
                     }

                     @Override
                     public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                     Integer lastVc = state.value() == null ? 0 : state.value();

                         System.out.println("lastVc---->"+lastVc);
                         System.out.println("valueGetVc---->"+value.getVc());

                     if (Math.abs(value.getVc() - lastVc) >= 10) {
                     out.collect(value.getId() + " 红色警报!!!");
                     }
                     state.update(value.getVc());
                     }
                     })
                .print();

        env.execute();


    }
}
