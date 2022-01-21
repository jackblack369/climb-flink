package brook.table.demo2;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * 使用 FlinkTable API 来实现订单的总笔数，最大金额和最小金额根据用户id
 */
public class Step6 {
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
        //3.Transformation 分配时间戳和水印2秒
        SingleOutputStreamOperator<Order> watermarkDS = source.assignTimestampsAndWatermarks(WatermarkStrategy.<Order>forBoundedOutOfOrderness(
                Duration.ofSeconds(2)
        ).withTimestampAssigner((element, recordTimestamp) -> element.createTime));

        //4.注册表 创建临时视图并在事件时间上分配 rowtime
        tEnv.createTemporaryView("t_order",
                watermarkDS,$("orderId"),$("userId"),$("money"),$("createTime").rowtime());
        //5.TableAPI查询
        // 获取 TableApi
        Table t_order = tEnv.from("t_order");
        //6.TableAPI 订单的统计，根据用户id 统计订单金额，最大金额和最小金额

        Table resultTable = t_order
                //6.1 根据窗口 window 分组，先有个滚动 window 窗口
                .window(Tumble.over(lit(5).second())
                        .on($("createTime")).as("tumbleWindow"))
                //6.2 对用户id 和 时间window 窗口 分组
                .groupBy($("tumbleWindow"), $("userId"))
                //6.3 查询出来对用订单总笔数和最大金额和最小金额
                .select($("userId"), $("orderId").count().as("totalCount")
                        , $("money").max().as("maxMoney")
                        , $("money").min().as("minMoney"));

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
