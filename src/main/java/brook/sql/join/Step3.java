package brook.sql.join;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * time window join
 * success
 */
public class Step3 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //source table：订单表
        String table1 = "CREATE TABLE t_order (" +
                "    order_id VARCHAR, " +
                "    product_id VARCHAR, " +
                "    create_time VARCHAR, " +
                "    order_proctime as PROCTIME()" +
                ") WITH (" +
                "    'connector.type' = 'kafka'," +
                "    'connector.version' = 'universal'," +
                "    'connector.topic' = 'order'," +
                "    'connector.startup-mode' = 'latest-offset'," +
                "    'connector.properties.zookeeper.connect' = 'localhost:2181'," +
                "    'connector.properties.bootstrap.servers' = 'localhost:9092'," +
                "    'update-mode' = 'append'," +
                "    'format.type' = 'json'," +
                "    'format.derive-schema' = 'true'" +
                ")";

        //source table: 产品表
        String table2 = "CREATE TABLE t_product (" +
                "    product_id VARCHAR, " +
                "    price DECIMAL(38,18), " +
                "    create_time VARCHAR, " +
                "    product_proctime as PROCTIME()" +
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

        //sink table：订购表
        String table3 = "CREATE TABLE order_detail (" +
                "    order_id VARCHAR," +
                "    product_id VARCHAR ," +
                "    price DECIMAL(38,18)," +
                "    order_create_time VARCHAR," +
                "    product_create_time VARCHAR," +
                "    order_proctime TIMESTAMP(3)," +
                "    product_proctime TIMESTAMP(3) " +
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
        
        //query sql
        String querySql = "INSERT INTO order_detail (order_id, product_id, price, order_create_time, product_create_time, order_proctime, product_proctime) " +
                " SELECT a.order_id, a.product_id, b.price, a.create_time, b.create_time, a.order_proctime, b.product_proctime" +
                " FROM t_order a" +
                " INNER JOIN t_product b ON a.product_id = b.product_id " +
                " and a.order_proctime BETWEEN b.product_proctime - INTERVAL '30' SECOND AND b.product_proctime + INTERVAL '30' SECOND " +
                " where a.order_id is not null" ;

        tableEnv.executeSql(table1);
        tableEnv.executeSql(table2);
        tableEnv.executeSql(table3);
        tableEnv.executeSql(querySql);

        tableEnv.executeSql("select * from order_detail").print();

        env.execute();

    }
}
