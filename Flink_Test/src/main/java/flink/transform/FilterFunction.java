package flink.transform;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class FilterFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment() ;
        env.fromElements("java","scala","test","scala","test","scala","test")
                .filter(new org.apache.flink.api.common.functions.FilterFunction<String>() {
                    public boolean filter(String s) throws Exception {
                        if(s.contains("java")){
                            return true ;
                        }
                        return false;


                    }
                }).print() ;

        env.addSource(new RichSourceFunction<String>() {
            public void run(SourceContext<String> sourceContext) throws Exception {

            }
            public void cancel() {
            }
        });


        env.execute() ;

    }


}
