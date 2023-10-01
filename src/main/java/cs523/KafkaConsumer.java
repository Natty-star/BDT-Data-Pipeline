package cs523;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import static cs523.KafkaConstant.KAFKA_BOOTSTRAP_SERVERS;
import static cs523.KafkaConstant.TOPIC_NAME;
import static cs523.EmployeeDataSet.*;
import static cs523.SparkHiveConstant.*;

public class KafkaConsumer {
    public static void main(String[] args) throws StreamingQueryException {

        //consume data from kafka and ingest to hive using spark
        SparkSession spark = SparkSession.builder()
                .appName("KafkaConsumer")
                .config("hive.metastore.uris", METADATA_URL)
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();

        Dataset<Row> ds = spark.readStream().format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", TOPIC_NAME)
                .load();

        Dataset<Row> lines = ds.selectExpr("CAST(value AS STRING)");
        Dataset<Row> dataset = processEmpDataSet(lines);

        dataset.writeStream()
                .foreachBatch((rowDataset, aLong) -> rowDataset
                        .write()
                        .mode(SaveMode.Append)
                        .insertInto(TABLE_NAME))
                .option("spark.sql.streaming.checkpointLocation", WAREHOUSE_DIR)
                .start()
                .awaitTermination();
    }

}
