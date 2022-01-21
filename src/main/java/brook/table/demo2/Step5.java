package brook.table.demo2;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;

import static org.apache.flink.table.api.Expressions.$;


/**
 * 使用Flink SQL来统计 5秒内 每个用户的 订单总数、订单的最大金额、订单的最小金额
 * 也就是每隔5秒统计最近5秒的每个用户的订单总数、订单的最大金额、订单的最小金额
 * 上面的需求使用流处理的Window的基于时间的滚动窗口就可以搞定!
 * 那么接下来使用FlinkTable&SQL-API来实现
 *
 */
public class Step5 {
    public static void main(String[] args) throws Exception {
        //1.准备环境 创建流执行环境和流表环境
        //准备流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置Flink table配置
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        //准备流表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        //2.Source 自定义Order 每一秒中睡眠一次
        DataStreamSource<Order> source = env.addSource(new MyOrder());
        //3.Transformation 分配时间戳和水印2秒, 添加水印，允许延迟2秒
        // ps：水印的策略是为了处理数据乱序的问题
        SingleOutputStreamOperator<Order> watermarkDS = source.assignTimestampsAndWatermarks(WatermarkStrategy.<Order>forBoundedOutOfOrderness(
                Duration.ofSeconds(2)
        ).withTimestampAssigner((element, recordTimestamp) -> element.createTime));

        //4.注册表 创建临时视图并在事件时间上分配 rowtime
        tEnv.createTemporaryView("t_order",
                watermarkDS,$("orderId"),$("userId"),$("money"),$("createTime").rowtime());
        //5.编写SQL，根据 userId 和 createTime 滚动分组统计 userId、订单总笔数、最大、最小金额
        String sql="SELECT userId,count(orderId) totalCount,max(money) maxMoney,min(money) minMoney " +
                "FROM t_order " +
                "group by userId," +
                "tumble(createTime,interval '5' second)"; //分组时要使用 tumble(时间列, interval '窗口时间' second) 来创建窗口
        //6.执行查询语句返回结果
        Table resultTable = tEnv.sqlQuery(sql);
        //7.Sink toRetractStream  → 将计算后的新的数据在DataStream原数据的基础上更新true或是删除false
        DataStream<Tuple2<Boolean, Row>> result = tEnv.toRetractStream(resultTable, Row.class);
        //8.打印输出
        result.print();
        //9.执行
        env.execute();
    }

    public static class MyOrder extends RichSourceFunction<Order> {
        Random rm = new Random();
        boolean flag = true;
        @Override
        public void run(SourceContext<Order> ctx) throws Exception {
            while(flag) {
                String oid = UUID.randomUUID().toString();
                int uid = rm.nextInt(3);
                int money = rm.nextInt(101);
                long createTime = System.currentTimeMillis();
                System.out.println(String.format("oid:%s, uid:%s, money:%s, creatTime:%s", oid, uid, money, createTime));
                //收集数据
                ctx.collect(new Order(oid, uid, money, createTime));
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    public static class Order {
        //订单id
        private String orderId;
        //用户id
        private Integer userId;
        //订单金额
        private Integer money;
        //事件时间
        private Long createTime;
    }

}
