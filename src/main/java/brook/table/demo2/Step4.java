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

import static org.apache.flink.table.api.Expressions.$;

/**
 * 使用 Flink Table,统计出来单词的出现次数为2 的单词的数据流打印输出
 */
public class Step4 {
    public static void main(String[] args) throws Exception {
        //1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //2.Source
        DataStream<WC> input = env.fromElements(
                new WC("Hello", 1),
                new WC("World", 1),
                new WC("Hello", 1)
        );
        //3.注册表
        Table table = tEnv.fromDataStream(input, $("word"), $("frequency"));

        //4.通过 FLinkTable API 过滤分组查询
        // select word,count(frequency) as frequency
        // from table
        // group by word
        // having count(frequency)=2;
        Table filter = table
                .groupBy($("word"))
                .select($("word"),
                        $("frequency").count().as("frequency"))
                .filter($("frequency").isEqual(2));

        //5.将结果集转换成 DataStream
        DataStream<Tuple2<Boolean, Row>> result = tEnv.toRetractStream(filter, Row.class);
        //6.打印输出
        result.print();
        //7.执行
        env.execute();
    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WC {
        public String word;
        public long frequency;
    }


}
