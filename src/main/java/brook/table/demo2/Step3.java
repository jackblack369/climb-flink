package brook.table.demo2;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 使用SQL方式对DataStream中的单词进行统计。
 */
public class Step3 {
    public static void main(String[] args) throws Exception{
            //1.准备环境 获取流执行环境 流表环境
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            //流表环境
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
            //2.Source 获取 单词信息
            DataStream<Order> orderA = env.fromCollection(Arrays.asList(
                    new Order(1L, "beer", 3),
                    new Order(1L, "diaper", 4),
                    new Order(3L, "rubber", 2)));

//            DataStream<Order> orderB = env.fromCollection(Arrays.asList(
//                    new Order(2L, "pen", 3),
//                    new Order(2L, "rubber", 3),
//                    new Order(4L, "beer", 1)));
            //3.创建视图 WordCount
            tEnv.createTemporaryView("t_order",orderA,$("user"),$("product"),$("amount"));
            //4.执行查询 根据用户统计订单总量
            Table table = tEnv.sqlQuery(
                    "select user,sum(amount) as totalAmount " +
                            " from t_order " +
                            " group by user "
            );
            //5.输出结果 retractStream获取数据流（别名）
            DataStream<Tuple2<Boolean, Row>> result = tEnv.toRetractStream(table, Row.class);
            //6.打印输出结果
            result.print();
            //7.执行
            env.execute();
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
