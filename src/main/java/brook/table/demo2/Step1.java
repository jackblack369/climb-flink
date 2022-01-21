package brook.table.demo2;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Step1 {
    public static void main(String[] args) {
        StreamExecutionEnvironment exeEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        // create a TableEnvironment for specific planner batch or streaming
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(exeEnv, settings);

        // create an input Table
        //tEnv.executeSql("CREATE TEMPORARY TABLE table1 ... WITH ( 'connector' = ... )");
        // register Orders table in table environment
        String creatSql = "CREATE TABLE KafkaTable (\n" +
                "  `user_id` BIGINT,\n" +
                "  `item_id` BIGINT,\n" +
                "  `behavior` STRING,\n" +
                "  `ts` TIMESTAMP(3) METADATA FROM 'timestamp'\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'user_behavior',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'csv'\n" +
                ")";
        tEnv.executeSql(creatSql);

//        Table result = tEnv.sqlQuery(
//                "SELECT user_id, item_id FROM KafkaTable WHERE behavior LIKE '%study%'");



// specify table program
        Table orders = tEnv.from("KafkaTable"); // schema (a, b, c, rowtime)

        String querySql = "select user_id, item_id, behavior from KafkaTable";

        tEnv.executeSql(querySql);

//        Table counts = orders
//                .groupBy($("item_id"))
//                .select($("item_id"), $("user_id").count().as("cnt"));
//        counts.execute();
    }
}
