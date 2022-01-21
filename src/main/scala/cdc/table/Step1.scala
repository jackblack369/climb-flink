package cdc.table

import com.ververica.cdc.connectors.mysql.MySqlSource
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

object Step1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 使用MySQLSource创建数据源
    // 同时指定StringDebeziumDeserializationSchema，将CDC转换为String类型输出
    val sourceFunction = MySqlSource.builder().hostname("172.20.2.6").port(3306)
      .databaseList("train_dongwei")
//      .tableList("source_one")
      .username("root").password("MySQL!23")
      .deserializer(new StringDebeziumDeserializationSchema).build();

    // 单并行度打印，避免输出乱序
    env.addSource(sourceFunction).print().setParallelism(1)

    env.execute()
  }

}
