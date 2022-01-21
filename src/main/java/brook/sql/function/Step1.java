package brook.sql.function;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 使用datagen生成测试数据（使用insert语句将datagen生成的数据插入到物理表中,在之后的SQL作业中使用物理表,不能直接使用datagen表,会报错）
 * 使用over聚合，计算每个账户过去1小时内的转出金额总和(当前这笔交易之前的一小时,包含当前交易)
 */
@Slf4j
public class Step1 {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String datagenSQL = "create table test_source( \n" +
                "  trans_id INT, \n" +
//                "  trans_date as FROM_UNIXTIME(1642660922 + trans_id, 'yyyy-MM-dd'), \n" +
//                "  trans_time as FROM_UNIXTIME(1642660922 + trans_id, 'HH:mm:ss'), \n" +
                "  trans_date as CURRENT_DATE, \n" +
                "  trans_time as CURRENT_TIME, \n" +
                "  account as CONCAT_WS('_', 'account_name', CAST(trans_id % 10 AS STRING)), \n" +
                "  amount as IF(trans_id%5 > 2, 10, 5), \n" +
                "  tran_direct as IF(trans_id%5 > 2, 'in', 'out'), \n" +
                "  target_account as CONCAT_WS('_', 'target_account_name', CAST(trans_id % 20 AS STRING)), \n" +
                "  tran_type_id as trans_id%5+1" +
                ") with (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second' = '15',\n" +
                " 'fields.trans_id.kind' = 'sequence',\n" +
                " 'fields.trans_id.start' = '1',\n" +
                " 'fields.trans_id.end' = '6000'\n" +
                ")";

        String kafkaSQL = "create table tran_source (\n" +
                " trans_id BIGINT,\n" +
                " trans_date_time TIMESTAMP(3),\n" +
                " account STRING,\n" +
                " amount BIGINT,\n" +
                " tran_direct STRING,\n" +
                " target_account STRING,\n" +
                " tran_type_id INT,\n" +
                "WATERMARK FOR trans_date_time AS trans_date_time - INTERVAL '10' SECOND\n" +
                ") WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'tran_source',\n" +
                "'properties.bootstrap.servers' = '127.0.0.1:9092',\n" +
                "'format' = 'json',\n" +
                "'scan.startup.mode'='latest-offset'\n" +
                ")";

//        String testSQL = "select TO_TIMESTAMP(CONCAT_WS(' ', CAST(trans_date as VARCHAR), CAST(trans_time as VARCHAR)), 'yyyy-MM-dd HH:mm:ss') as trans_date_time from test_source";

        String initKafkaData = "insert into tran_source " +
                "select trans_id,TO_TIMESTAMP(CONCAT_WS(' ', CAST(trans_date as VARCHAR), CAST(trans_time as VARCHAR)), 'yyyy-MM-dd HH:mm:ss') as trans_date_time, account, amount, tran_direct, target_account, tran_type_id from test_source";

        String overSQL = "SELECT account,\n" +
                "  SUM(amount) OVER (\n" +
                "    PARTITION BY account\n" +
                "    ORDER BY trans_date_time\n" +
                "    RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW\n" +
                "  ) AS one_hour_account_amount_sum\n" +
                " FROM tran_source where tran_direct='out'";

        tableEnv.executeSql(datagenSQL);
        tableEnv.executeSql(kafkaSQL);

        //        tableEnv.executeSql("select * from test_source").print();
//                tableEnv.executeSql(testSQL).print();

        tableEnv.executeSql(initKafkaData);
        tableEnv.executeSql(overSQL).print();

        env.execute();
    }
}
