//todo 3)自定义函数TableProcessFunction-基本信息定义
package project_test1.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import project_test1.gmall.realtime.bean.TableProcess;
import project_test1.gmall.realtime.common.GmallConfig;
import project_test1.gmall.realtime.utils.MysqlUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TableProcessFunction extends ProcessFunction {
    //因为要将维度数据写到侧输出流，所以要一个侧输出流标签
    private OutputTag<JSONObject> outputTag ;
    public TableProcessFunction(OutputTag<JSONObject> outputTag){
        this.outputTag = outputTag ;
    }

    //用于在内存中存储配置表对象 [表名，表配置信息]
    private Map<String, TableProcess> tableProcessMap = new HashMap<>() ;

    //表示目前内存中已经存在的Hbase表
    private Set<String> existsTables = new HashSet<>() ;

    //声明Phoneix连接
    private Connection connection =null ;

    //todo 4)自定义函数TableProcessFuntion-open
    //生命周期方法，初始化连接，初始化配置表信息并开启定时任务，用于不断读取配置表信息
    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化Phoenix连接
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver") ;
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER) ;

        //初始化配置表信息


        super.open(parameters);
    }


    //todo 5）自定义函数TableProcessFunction-iniTableProcessMap
    //读取MySQL中的配置表数据
    private void initTableProcessMap(){
        System.out.println("更新配置的处理信息");
        //查询MySQL中的配置数据
        List<TableProcess> tableProcessList = MysqlUtil.queryList("select * from table_process", TableProcess.class, true);
        //遍历查询结果，将数据存入结果集合
        for (TableProcess tableProcess: tableProcessList) {
            //获取源表表名
            String sourceTable = tableProcess.getSourceTable();
            //获取数据操作类型
            String operateType = tableProcess.getOperateType();
            //获取结果表表名
            String sinkTable = tableProcess.getSinkTable();
            //获取sink类型
            String sinkType = tableProcess.getSinkType();
            //拼接字段创建主键
            String key = sourceTable + ":" + operateType;
            //将数据存入结果集合
            tableProcessMap.put(key,tableProcess) ;
            //如果是向HBASE这中保存的表，那么这判断在内存中维护的
            if("insert".equals(operateType) && "hbase".equals(sinkType)) {
                boolean notExist = existsTables.add(sourceTable);
                //如果表信息不存在内存，则在Phoenix中创建新的表
                if (notExist) {
                    checkTable(sinkTable,tableProcess.getSinkColumns(),tableProcess.getSinkPk(),tableProcess.getSinkExtend());
                }
            }
        }
        if (tableProcessMap == null || tableProcessMap.size() == 0) {
            throw new RuntimeException("缺少处理信息") ;
        }
    }

    //todo 6）自定义函数TableProcessFunction-checkTable
    //如果mysql的配置表中添加了数据，该方法用于检查HBase中是否创建过表，如果没有则通过Phoenix中创建新增的表
    private void checkTable(String tableName,String fields ,String pk ,String ext) {
        //主键不存在，则给定默认值
        if(pk == null ) {
            pk="id" ;
        }
        //扩展字段不存在，则给定默认值
        if(ext == null ){
            ext = "" ;
        }
        //创建字符串拼接对象，用于拼接建表语句SQL
        StringBuilder createSql = new StringBuilder("create table is not exists" + GmallConfig.HBASE_SCHEMA + "." + tableName + "(");
        //将列做切分，并拼接至建表语句SQL中
        String[] fieldsArr = fields.split(",");
        for (int i=0; i<fields.length();i++) {
            String field = fieldsArr[i];
            if (pk.equals(field)) {
                createSql.append(field).append(" varchar") ;
                createSql.append(" primary key ") ;
            } else {
                createSql.append("info.").append(field).append(" varchar") ;
            }
            if (i < fieldsArr.length -1) {
                createSql.append(",") ;
            }
        }
        createSql.append(" )") ;
        createSql.append(ext) ;

        try {
            //执行建表语句在Phoenix中创建表
            System.out.println(createSql);
            PreparedStatement ps = connection.prepareStatement(createSql.toString());
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("建表失败！！！！") ;
        }
    }

    //todo 8)自定义函数TableProcessFunction-filterColumn()
    //校验字段，过滤多余的字段
    private void filterColumn(JSONObject data ,String sinkColumns) {
        String[] cols = StringUtils.split(sinkColumns, ",");
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        List<String> columnList = Arrays.asList(cols) ;
        for (Iterator<Map.Entry<String,Object>> iterator = entries.iterator();iterator.hasNext(); ) {
            Map.Entry<String,Object> entry = iterator.next() ;
            if (!columnList.contains(entry.getKey())){
                iterator.remove();
            }
        }
    }

    //todo 9)?????自定义函数TableProcessFunction-processElement()
    //核心处理方法，根据mysql配置表的信息为每条数据打标签，走kafka还是Hbase
    @Override
    public void processElement(Object value, Context ctx, Collector out) throws Exception {

    }
}
