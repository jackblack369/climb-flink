package brook.table.demo2;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;


/**
 * 将两个数据流 DataStream 通过 FlinkTable & SQL API 进行 union all 操作
 */
public class Step2 {
    public static void main(String[] args) throws Exception{
        //1.准备环境 创建流环境 和 流表环境，并行度设置为1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        //创建流表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,settings);
        //2.Source 创建数据集
        DataStream<Order> orderA = env.fromCollection(Arrays.asList(
                new Order(1L, "beer", 3),
                new Order(1L, "diaper", 4),
                new Order(3L, "rubber", 2)));

        DataStream<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(2L, "pen", 3),
                new Order(2L, "rubber", 3),
                new Order(4L, "beer", 1)));
        //3.注册表 将数据流转换成表
        // 通过fromDataStream将数据流转换成表
        Table orderTableA = tEnv.fromDataStream(orderA, $("user"), $("product"), $("amount"));
        // 将数据流转换成 创建临时视图
        tEnv.createTemporaryView("orderTableB",orderB,$("user"), $("product"), $("amount"));
        //4.执行查询，查询order1的amount>2并union all 上 order2的amoun<2的数据生成表
        Table result = tEnv.sqlQuery("" +
                "select * from " + orderTableA + " where amount>2 " +
                "union all " +
                "select * from orderTableB where amount<2");
        //4.1 将结果表转换成toAppendStream数据流
        //字段的名称和类型
        result.printSchema();
        DataStream<Row> resultDS = tEnv.toAppendStream(result, Row.class);
        //5.打印结果
        resultDS.print();
        //6.执行环境
        env.execute();
        // 创建实体类 user:Long product:String amount:int
    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        public Long user;
        public String product;
        public int amount;
    }

}
