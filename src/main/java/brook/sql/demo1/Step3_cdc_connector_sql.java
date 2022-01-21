package brook.sql.demo1;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * SUCCESS
 */
public class Step3_cdc_connector_sql {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.创建 Flink-MySQL-CDC 的 Source
        String sourceDDL ="CREATE TABLE source_one (" +
                " id INT," +
                " name STRING," +
                "PRIMARY KEY(id) NOT ENFORCED" +
                ") WITH (" +
                " 'connector' = 'mysql-cdc'," +
                " 'hostname' = '127.0.0.1'," +
                " 'port' = '3306'," +
                " 'username' = 'root'," +
                " 'password' = '1qaz2wsx'," +
                " 'database-name' = 'cdc'," +
                " 'table-name' = 'source_one'" +
                ")";

        //3. 创建kafka
        String sourceKafka = "CREATE TABLE mysql2kafka (" +
                "  id INT," +
                "  name STRING" +
                ") WITH (" +
                " 'connector' = 'kafka'," +
                " 'topic' = 'mysql2kafka'," +
                " 'scan.startup.mode' = 'earliest-offset'," +
                " 'properties.bootstrap.servers' = '127.0.0.1:9092'," +
                " 'format' = 'debezium-json'" +
                ")";

        tableEnv.executeSql(sourceDDL);
        tableEnv.executeSql(sourceKafka);

        tableEnv.executeSql("insert into mysql2kafka select id,name from source_one").print();
        env.execute();
    }
}
