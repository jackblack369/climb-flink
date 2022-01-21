package brook.sql.demo1;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * SUCCESS
 */
public class Step2_cdc_connector_sql {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.创建 Flink-MySQL-CDC 的 Source
        tableEnv.executeSql("CREATE TABLE source_one (" +
                " id INT," +
                " name STRING," +
                "PRIMARY KEY(id) NOT ENFORCED" +
                ") WITH (" +
                " 'connector' = 'mysql-cdc'," +
                " 'hostname' = '172.20.2.6'," +
                " 'port' = '3306'," +
                " 'username' = 'root'," +
                " 'password' = 'MySQL!23'," +
                " 'database-name' = 'train_dongwei'," +
                " 'table-name' = 'source_one'" +
                ")");
        tableEnv.executeSql("select * from source_one").print();
        env.execute();
    }
}
