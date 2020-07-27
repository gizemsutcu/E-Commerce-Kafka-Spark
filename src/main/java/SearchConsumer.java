import com.mongodb.spark.MongoSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SearchConsumer {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\hadoop-common-2.2.0-bin-master");

        StructType schema = new StructType()
                .add("userId",DataTypes.IntegerType)
                .add("search", DataTypes.StringType)
                .add("region",DataTypes.StringType)
                .add("current_ts",DataTypes.StringType);

        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("Spark Search Analysis")
                .config("spark.mongodb.output.uri","mongodb://your_host_ip/database_name.collection_name")
                .getOrCreate();

        Dataset<Row> loadDS = sparkSession.read().format("kafka")
                .option("kafka.bootstrap.servers", "your_host_ip:9092")
                .option("subscribe", "topic_name")
                .load();

        Dataset<Row> rowDataset = loadDS.selectExpr("CAST(value AS STRING)");
        Dataset<Row> valueDS = rowDataset.select(functions.from_json(rowDataset.col("value"), schema).as("jsontostructs")).select("jsontostructs.*");
        //valueDS.printSchema();
        //valueDS.show();
        //System.out.println(valueDS.count());

        //Top 10 most searched products daily --> MongoDB
        Dataset<Row> searchGroupData = valueDS.groupBy("search").count().sort(functions.desc("count")).limit(10);
        MongoSpark.write(searchGroupData).mode("overwrite").save();

        //Number of searches of products by userId --> MongoDB
        Dataset<Row> countDS = valueDS.groupBy("userId", "search").count();
        Dataset<Row> filterDS = countDS.filter("count > 3");
        Dataset<Row> pivot = filterDS.groupBy("userId").pivot("search").sum("count").na().fill(0);
        MongoSpark.write(pivot).option("collection","searchByUserId").mode("overwrite").save();

        //the number of searches for products in half an hour period --> MongoDB
        Dataset<Row> current_ts_window = valueDS.groupBy(functions.window(valueDS.col("current_ts"), "30 minute"), valueDS.col("search")).count();
        MongoSpark.write(current_ts_window).option("collection","timeWindowSearch").save();

    }
}
