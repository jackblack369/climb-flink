package brook.kafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

public class ConnectKafka {
    public static void main(String[] args) throws Exception{
        // 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.创建TableEnvironment(Blink planner)
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env , settings);

        // Kafka Consumer
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        // 消费者组（可以使用消费者组将若干个消费者组织到一起），共同消费Kafka中topic的数据
        // 每一个消费者需要指定一个消费者组，如果消费者的组名是一样的，表示这几个消费者是一个组中的
        properties.setProperty("group.id","flink");

        // kafka数据源
        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>
                ("flinkStep", new SimpleStringSchema(), properties));

        dataStream.map(new MapFunction<String, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(String line) throws Exception {
                String[] words = line.split(",");
                return new Tuple2<>(words[0],words[1]);
            }
        }).print();

        env.execute("kafka");

    }
}
