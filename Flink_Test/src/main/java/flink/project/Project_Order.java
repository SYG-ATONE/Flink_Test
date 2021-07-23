package flink.project;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class Project_Order {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<OrderEvent> orderEventSingleOutputStreamOperator = env.readTextFile("input/OrderLog.csv")
                .map(line -> {
                    String[] split = line.split(",");
                    return new OrderEvent(
                            Long.valueOf(split[0]),
                            split[1],
                            split[2],
                            Long.valueOf(split[3])
                    );
                });
        SingleOutputStreamOperator<TxEvent> txEventSingleOutputStreamOperator = env.readTextFile("input/ReceiptLog.csv")
                .map(line -> {
                    String[] split = line.split(",");

                    return new TxEvent(
                            split[0],
                            split[1],
                            Long.valueOf(split[2])
                    );
                });

        ConnectedStreams<OrderEvent, TxEvent> orderAndTx = orderEventSingleOutputStreamOperator.connect(txEventSingleOutputStreamOperator);

        orderAndTx.keyBy("txId","txId")
                .process(
                        new CoProcessFunction<OrderEvent, TxEvent, String>() {

                            //存txId -> OrderEvent
                            HashMap<String,OrderEvent>  orderMap = new HashMap<>() ;

                            //存txId -> TxEvent
                            HashMap<String,TxEvent> txMap = new HashMap<>() ;

                            @Override
                            public void processElement1(OrderEvent orderEvent, Context context, Collector<String> collector) throws Exception {

                                //获取交易信息
                                if (txMap.containsKey(orderEvent.getTxId())) {
                                    collector.collect("订单：" +orderEvent);
                                    txMap.remove(orderEvent.getTxId()) ;
                                } else {
                                    orderMap.put(orderEvent.getTxId(),orderEvent) ;
                                }
                            }

                            @Override
                            public void processElement2(TxEvent txEvent, Context context, Collector<String> collector) throws Exception {

                                //获取订单信息
                                if (orderMap.containsKey(txEvent.getTxId())){
                                    OrderEvent orderEvent = orderMap.get(txEvent) ;
                                    collector.collect("订单："+orderEvent+" 对账成功");
                                    orderMap.remove(txEvent.getTxId()) ;
                                }else {
                                    txMap.put(txEvent.getTxId(),txEvent) ;
                                }
                            }
                        }
                )
                .print();
        env.execute() ;

    }
}
