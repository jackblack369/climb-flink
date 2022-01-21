package brook.sql.catalog;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;

import java.util.Arrays;
import java.util.HashMap;

@Slf4j
public class Step1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExec = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamExec);

        Catalog customCatalog = new GenericInMemoryCatalog("test_catalog");


        tableEnv.registerCatalog("train_catalog", customCatalog);

        customCatalog.createDatabase("train_database", createDb(), false);

        log.info("catalogs:[{}]" , Arrays.toString(tableEnv.listCatalogs()));
        log.info("databases:[{}]" , Arrays.toString(tableEnv.listDatabases()));
        log.info("train databases:[{}]" , ArrayUtils.toString(customCatalog.listDatabases()));
        log.info("talbes:[{}]", Arrays.toString(tableEnv.listTables()));

//        streamExec.execute();

    }

    public static CatalogDatabase createDb() {
        return new CatalogDatabaseImpl(
                new HashMap<String, String>() {
                    {
                        put("k1", "v1");
                    }
                },
                "test_comment");
    }
}
