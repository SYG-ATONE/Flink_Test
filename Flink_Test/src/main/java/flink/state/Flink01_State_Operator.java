package flink.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink01_State_Operator {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment() ;

        env.socketTextStream("192.168.2.21",9999)
                .map(new MyCountMapper()) ;
    }


    private static class MyCountMapper implements MapFunction<String,Long>, CheckpointedFunction {

        private Long count = 0L ;
        private ListState<Long> state ;

        @Override
        public Long map(String s) throws Exception {
            count++ ;
            return count;
        }
        //Checkpoint时会调用这个方法，我们要实现snapshot逻辑，比如将哪些本地状态持久化
        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            state.clear();
            state.add(count);

        }

        //初始化会调用这个方法，向本地状态中填充数据，每个子任务调用一次。或者从之前的检查点恢复时也会调用这个方法
        @Override
        public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {


            Class<Long> test = (Class<Long>) Class.forName("java.lang.Long");
            state = functionInitializationContext
                    .getOperatorStateStore()
                    .getListState(new ListStateDescriptor<Long>("state",Long.class)) ;

            for (Long c: state.get()){
                count += c ;
            }

        }
    }

}




