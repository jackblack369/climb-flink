package brook.sql.join;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;

import java.util.HashMap;

/**
 * regular join
 * not success
 */
public class Step1 {
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
        String ddlStream1 ="CREATE TABLE trainCatalog.trainDatabase.trainStream1 (" +
                "  id INT COMMENT 'primary ID',   " +
                "  name STRING," +
                "  click_timestamp BIGINT," +
                "  procTime AS PROCTIME(), " +
                "  ets AS TO_TIMESTAMP(FROM_UNIXTIME(click_timestamp / 1000))," +
                "  WATERMARK FOR ets AS ets - INTERVAL '10' SECOND" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'trainTopic1'," +
//                "  'properties.group.id' = 'train_group'," +
                "  'properties.bootstrap.servers' = '127.0.0.1:9092'," +
                "  'format' = 'json'" +
                ")";

        //创建 stream table 2
        String ddlStream2 = "CREATE TABLE trainCatalog.trainDatabase.trainStream2 (" +
                "  id INT, " +
                "  referenceName STRING," +
                "  watch_timestamp BIGINT," +
                "  ets AS TO_TIMESTAMP(FROM_UNIXTIME(watch_timestamp / 1000))," +
                "  WATERMARK FOR ets AS ets - INTERVAL '10' SECOND" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'trainTopic2'," +
//                "  'properties.group.id' = 'train_group'," +
                "  'properties.bootstrap.servers' = '127.0.0.1:9092'," +
                "  'format' = 'json'" +
                ")";

        String querySql = "select table1.id as sourceId, table2.referenceName as referenceName" +
                " from trainCatalog.trainDatabase.trainStream1 as table1 " +
                " inner join trainCatalog.trainDatabase.trainStream2 as table2 " +
                " on table1.id = table2.id";

        tableEnv.executeSql(ddlStream1);
        tableEnv.executeSql(ddlStream2);
        tableEnv.executeSql(querySql).print(); //todo select没有返回结果

        env.execute();
    }
}
