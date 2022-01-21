package brook.table.demo1;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.TableDescriptor;
import org.apache.flink.table.factories.DataGenOptions;

public class StepOne {
    public static void main(String[] args) {
/*        // create a TableEnvironment for specific planner batch or streaming
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        // or TableEnvironment tableEnv = TableEnvironment.create(bsSettings);

// create an input Table
        //tableEnv.executeSql("CREATE TEMPORARY TABLE table1 ... WITH ( 'connector' = ... )");
        tableEnv.executeSql("CREATE TEMPORARY TABLE SinkTable WITH ('connector' = 'blackhole') LIKE SourceTable");
// register an output Table
        tableEnv.executeSql("CREATE TEMPORARY TABLE outputTable ... WITH ( 'connector' = ... )");

// create a Table object from a Table API query
        Table table2 = tableEnv.from("table1").select(...);
// create a Table object from a SQL query
        Table table3 = tableEnv.sqlQuery("SELECT ... FROM table1 ... ");

// emit a Table API result Table to a TableSink, same for SQL result
        TableResult tableResult = table2.executeInsert("outputTable");
        tableResult...*/
    }
}
