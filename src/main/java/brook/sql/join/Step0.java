package brook.sql.join;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;

import java.util.HashMap;

public class Step0 {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Catalog trainCatalog = new GenericInMemoryCatalog("trainCatalog");
        tableEnv.registerCatalog("trainCatalog", trainCatalog);
        trainCatalog.createDatabase("trainDatabase", new CatalogDatabaseImpl(
                new HashMap<String, String>() {
                    {
                        put("name", "dongwei");
                    }
                },
                "test_comment"
        ), false);



        //创建 stream table 1
//        String ddlStream1 ="CREATE TABLE trainCatalog.trainDatabase.trainStream1 (" +
//                "  id INT COMMENT 'primary ID',   " +
//                "  name STRING," +
//                "  click_timestamp BIGINT," +
//                "  procTime AS PROCTIME(), " +
//                "  ets AS TO_TIMESTAMP(FROM_UNIXTIME(click_timestamp / 1000))," +
//                "  WATERMARK FOR ets AS ets - INTERVAL '10' SECOND" +
//                ") WITH (" +
//                "  'connector' = 'kafka'," +
//                "  'topic' = 'trainTopic1'," +
////                "  'properties.group.id' = 'train_group'," +
//                "  'properties.bootstrap.servers' = '127.0.0.1:9092'," +
//                "  'format' = 'json'" +
//                ")";




        String table2 = "CREATE TABLE trainCatalog.trainDatabase.t_product (" +
                "    product_id VARCHAR, " +
                "    price DECIMAL(38,18), " +
                "    create_time VARCHAR " +
                ") WITH (" +
                "    'connector.type' = 'kafka'," +
                "    'connector.version' = 'universal'," +
                "    'connector.topic' = 'shipments'," +
                "    'connector.startup-mode' = 'earliest-offset'," + //latest-offset
                "    'connector.properties.zookeeper.connect' = 'localhost:2181'," +
                "    'connector.properties.bootstrap.servers' = 'localhost:9092'," +
                "    'update-mode' = 'append'," +
                "    'format.type' = 'json'," +
                "    'format.derive-schema' = 'true'" +
                ")";

//        tableEnv.executeSql(table1);
        tableEnv.executeSql(table2);
//        tableEnv.executeSql("select * from trainCatalog.trainDatabase.t_order").print();
        tableEnv.executeSql("select * from trainCatalog.trainDatabase.t_product").print();
        env.execute();
    }
}
