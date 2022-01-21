package brook.sql.join;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;

import java.util.HashMap;

/**
 * regular join
 * success
 */
public class Step2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);

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

        //source table
        String table1 = "CREATE TABLE trainCatalog.trainDatabase.t_order(" +
                "    order_id VARCHAR, " +
                "    product_id VARCHAR, " +
                "    create_time VARCHAR " +
                ") WITH (" +
                "    'connector.type' = 'kafka'," +
                "    'connector.version' = 'universal'," +
                "    'connector.topic' = 'order'," +
                "    'connector.startup-mode' = 'earliest-offset'," + //earliest-offset
                "    'connector.properties.0.key' = 'zookeeper.connect'," +
                "    'connector.properties.0.value' = 'localhost:2181'," +
                "    'connector.properties.1.key' = 'bootstrap.servers'," +
                "    'connector.properties.1.value' = 'localhost:9092'," +
                "    'update-mode' = 'append'," +
                "    'format.type' = 'json'," +
                "    'format.derive-schema' = 'true'" +
                ")";
         
        String table2 = "CREATE TABLE trainCatalog.trainDatabase.t_product (" +
                "    product_id VARCHAR, " +
                "    price DECIMAL(38,18), " +
                "    create_time VARCHAR " +
                ") WITH (" +
                "    'connector.type' = 'kafka'," +
                "    'connector.version' = 'universal'," +
                "    'connector.topic' = 'shipments'," +
                "    'connector.startup-mode' = 'latest-offset'," +
                "    'connector.properties.zookeeper.connect' = 'localhost:2181'," +
                "    'connector.properties.bootstrap.servers' = 'localhost:9092'," +
                "    'update-mode' = 'append'," +
                "    'format.type' = 'json'," +
                "    'format.derive-schema' = 'true'" +
                ")";

        //sink table
        String table3 = "CREATE TABLE trainCatalog.trainDatabase.order_detail (" +
                "    order_id VARCHAR," +
                "    producer_id VARCHAR ," +
                "    price DECIMAL(38,18)," +
                "    order_create_time VARCHAR," +
                "    product_create_time VARCHAR" +
                ") WITH (" +
                "    'connector.type' = 'kafka'," +
                "    'connector.version' = 'universal'," +
                "    'connector.topic' = 'order_detail'," +
                "    'connector.startup-mode' = 'latest-offset'," +
                "    'connector.properties.zookeeper.connect' = 'localhost:2181'," +
                "    'connector.properties.bootstrap.servers' = 'localhost:9092'," +
                "    'update-mode' = 'append'," +
                "    'format.type' = 'json'," +
                "    'format.derive-schema' = 'true'" +
                ")";

        String insertSql = "INSERT INTO trainCatalog.trainDatabase.order_detail " +
                " SELECT a.order_id, a.product_id, b.price, a.create_time, b.create_time" +
                " FROM trainCatalog.trainDatabase.t_order a" +
                "  INNER JOIN trainCatalog.trainDatabase.t_product b ON a.product_id = b.product_id" +
                " where a.order_id is not null";

        tableEnv.executeSql(table1);
        tableEnv.executeSql(table2);
        tableEnv.executeSql(table3);
        tableEnv.executeSql(insertSql);

        tableEnv.executeSql("select * from order_detail").print();

        streamEnv.execute();


    }
}
