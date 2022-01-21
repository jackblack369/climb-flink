package brook.sql.function;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 为每一笔交易记录添加交易类型名(根据交易类型id与tran_type_table关联,获取交易类型名)
 * join操作
 */
@Slf4j
public class Step3 {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String datagenSQL = "create table test_source( \n" +
                "  trans_id INT, \n" +
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
                "WATERMARK FOR trans_date_time AS trans_date_time -1 INTERVAL '1' MINUTE\n" +
                ") WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'tran_source',\n" +
                "'properties.bootstrap.servers' = '127.0.0.1:9092',\n" +
                "'format' = 'json',\n" +
                "'scan.startup.mode'='latest-offset'\n" +
                ")";

        String initKafkaData = "insert into tran_source " +
                "select trans_id,TO_TIMESTAMP(CONCAT_WS(' ', CAST(trans_date as VARCHAR), CAST(trans_time as VARCHAR)), 'yyyy-MM-dd HH:mm:ss') as trans_date_time, account, amount, tran_direct, target_account, tran_type_id from test_source";

        String dimensionSQL = "create table tran_type_table(\n" +
                " tran_type_id, \n" +
                " Tran_type_name \n" + //交易类型名(1:转账2:汇款3:消费4:工资5:其他) )
                ") WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'tran_source',\n" +
                "'properties.bootstrap.servers' = '127.0.0.1:9092',\n" +
                "'format' = 'json',\n" +
                "'scan.startup.mode'='latest-offset'\n" +
                ")";


        tableEnv.executeSql(datagenSQL);
        tableEnv.executeSql(kafkaSQL);

        //        tableEnv.executeSql("select * from test_source").print();
        //        tableEnv.executeSql(testSQL).print();

        tableEnv.executeSql(initKafkaData);

        env.execute();
    }
}
