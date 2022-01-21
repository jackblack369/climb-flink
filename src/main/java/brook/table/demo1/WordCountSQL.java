package brook.table.demo1;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class WordCountSQL {
    public static void main(String[] args) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

        // 在表环境中注册 Orders 表



        // 指定表程序
        Table orders = tEnv.from("Orders"); // schema (a, b, c, rowtime)

        Table counts = orders
                .groupBy($("a"))
                .select($("a"), $("b").count().as("cnt"));

        // 打印
        counts.execute().print();
    }
}
