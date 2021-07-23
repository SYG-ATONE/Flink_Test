package project_test1.gmall.realtime.common;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;

import java.util.*;
 //todo 7)定义项目中常用的配置常量类GmallConfig
public class GmallConfig {
    public static final String HBASE_SCHEMA = "GMALL2021_REALTIME";
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181" ;

}
