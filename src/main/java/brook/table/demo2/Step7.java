package brook.table.demo2;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 将kafka中的json字符串映射成一张Flink表，对这张表进行过滤分组聚合操作之后落地到 Kafka的表中
 * 如果不用 FlinkTable 直接使用 Flink DataStream 能做吗？
 *
 * 读取 Kafka 数据源 FlinkKafkaConsumer
 * 将Json字符串转换成 Java Bean
 * Flink的 filter算子 进行过滤 .filter(t->t.status.equal("success"))
 * 将对象 map 转换成 JSON.toJsonString => json string
 * 写入 Kafka FlinkKafkaProducer
 *
 * 使用 Flink TableApi 来实现过滤 status="status"
 */
public class Step7 {
    public static void main(String[] args) throws Exception {
        //1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建流表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //2.Source
        //从 kafka 中直接映射到输入表
        TableResult inputTable = tEnv.executeSql(
                "CREATE TABLE input_kafka (\n" +
                        "  `user_id` BIGINT,\n" +
                        "  `page_id` BIGINT,\n" +
                        "  `status` STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +  //连接的数据源是 kafka
                        "  'topic' = 'input_kafka',\n" + //映射的主题topic
                        "  'properties.bootstrap.servers' = 'localhost:9092',\n" + //kafka地址
                        "  'properties.group.id' = 'default',\n" + //kafka消费的消费组
                        "  'scan.startup.mode' = 'latest-offset',\n" + //从最新的位置扫描
                        "  'format' = 'json'\n" +  //扫描的数据是格式：json格式
                        ")"
        );
        //从 kafka 中映射一个输出表
        TableResult outputTable = tEnv.executeSql(
                "CREATE TABLE output_kafka (\n" +
                        "  `user_id` BIGINT,\n" +
                        "  `page_id` BIGINT,\n" +
                        "  `status` STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'output_kafka',\n" +
                        "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                        "  'format' = 'json',\n" +
                        "  'sink.partitioner' = 'round-robin'\n" +  //分区的方式，轮训
                        ")"
        );

        String sql = "select " +
                "user_id," +
                "page_id," +
                "status " +
                "from input_kafka " +
                "where status = 'success'";

        Table ResultTable = tEnv.sqlQuery(sql);

        DataStream<Tuple2<Boolean, Row>> resultDS = tEnv.toRetractStream(ResultTable, Row.class);
        resultDS.print();

        //将满足 status = 'success' 的记录存储到 output_kafka 落地表中
        tEnv.executeSql("insert into output_kafka select * from "+ResultTable);

        //7.excute
        env.execute();
    }

}
