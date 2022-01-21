package brook.table.demo3;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.table.KafkaOptions;

public class Step2_cdc_connector {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.Flink-CDC 将读取 binlog 的位置信息以状态的方式保存在 CK,如果想要做到断点
        //续传,需要从 Checkpoint 或者 Savepoint 启动程序
        //2.1 开启 Checkpoint,每隔 5 秒钟做一次 CK
        env.enableCheckpointing(5000L);
        //2.2 指定 CK 的一致性语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //2.3 设置任务关闭的时候保留最后一次 CK 数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 指定从 CK 自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
        //2.5 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://127.0.0.1:8020/flinkCDC"));
        //2.6 设置访问 HDFS 的用户名
        System.setProperty("HADOOP_USER_NAME", "dongwei");
        //3.创建 Flink-MySQL-CDC 的 Source
/*
        initial (default): Performs an initial snapshot on the monitored database tables upon
        first startup, and continue to read the latest binlog.
        latest-offset: Never to perform snapshot on the monitored database tables upon first
        startup, just read from the end of the binlog which means only have the changes since the
        connector was started.
        timestamp: Never to perform snapshot on the monitored database tables upon first
        startup, and directly read binlog from the specified timestamp. The consumer will traverse the
        binlog from the beginning and ignore change events whose timestamp is smaller than the
        specified timestamp.
        specific-offset: Never to perform snapshot on the monitored database tables upon
        first startup, and directly read binlog from the specified offset.
*/
        DebeziumSourceFunction<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("hadoop01")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall-flink")
                .tableList("gmall-flink.z_user_info") //可选配置项,如果不指定该参数,则会
        //读取上一个配置下的所有表的数据，注意：指定的时候需要使用"db.table"的方式
                .startupOptions(StartupOptions.initial())
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();
        //4.使用 CDC Source 从 MySQL 读取数据
        DataStreamSource<String> mysqlDS = env.addSource(mysqlSource);
        //5.打印数据
        mysqlDS.print();
        //6.执行任务
        env.execute();
    }
}
