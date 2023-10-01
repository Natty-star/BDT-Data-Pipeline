package cs523;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static cs523.SparkHiveConstant.*;
import static cs523.SparkHiveConstant.WAREHOUSE_DIR;

public class SparkHiveQuery {

    public static void main(String[] args) {

        //query data from hive using spark
        final SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.set("hive.metastore.uris", METADATA_URL);

        SparkSession sparkSession = SparkSession
                .builder()
                .config(sparkConf)
                .appName("SparkSQLHive")
                .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
                .enableHiveSupport()
                .getOrCreate();

        sparkSession.sql(HIVE_DB);

        Dataset<Row> rowDataset = sparkSession.sql(LOAD_ALL_RECORDS_SQL);
        rowDataset.show();

        //query complex query from hive using spark
        rowDataset = sparkSession.sql(COMPLEX_QUERY);
        rowDataset.show();

        rowDataset = sparkSession.sql(COMPLEX_QUERY_1);
        rowDataset.show();

        rowDataset = sparkSession.sql(COMPLEX_QUERY_2);
        rowDataset.show();
    }

}
