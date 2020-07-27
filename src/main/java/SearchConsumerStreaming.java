import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SearchConsumerStreaming {
    public static void main(String[] args) throws StreamingQueryException {
        System.setProperty("hadoop.home.dir","C:\\hadoop-common-2.2.0-bin-master");

        StructType schema = new StructType()
                .add("userId", DataTypes.IntegerType)
                .add("search", DataTypes.StringType)
                .add("region",DataTypes.StringType)
                .add("current_ts",DataTypes.StringType);

        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("Spark Search Analysis")
                .config("spark.mongodb.output.uri","mongodb://your_host_ip/database_name.collection_name")
                .getOrCreate();

        Dataset<Row> loadDS = sparkSession.readStream().format("kafka")
                .option("kafka.bootstrap.servers", "your_host_ip:9092")
                .option("subscribe", "topic_name")
                .load();

        Dataset<Row> rowDataset = loadDS.selectExpr("CAST(value AS STRING)");
        Dataset<Row> valueDS = rowDataset.select(functions.from_json(rowDataset.col("value"), schema).as("data")).select("data.*");

        Dataset<Row> maskeFilter = valueDS.filter(valueDS.col("search").equalTo("maske"));

        //Saving query result to mongodb
        //you can use the trigger function for the time interval -->  trigger(Trigger.ProcessingTime(60000))
        maskeFilter.writeStream().foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
            @Override
            public void call(Dataset<Row> rowDataset, Long aLong) throws Exception {
                MongoSpark.write(rowDataset).option("collection","searchWithMaske").mode("append").save();
            }
        }).start().awaitTermination();

        //Query that brings the same query to the console instead of saving it to mongoDB
        maskeFilter.writeStream().format("console").outputMode("append").start().awaitTermination();

    }
}
