import Event.event;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.spark.sql.*;
import java.util.Objects;

public class main {
    public static void main(String[] args) throws AnalysisException {
        String filePath = "./data.dat";
        SparkSession spark = SparkSession
                .builder()
                .appName("first")
                .master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        // read file use encoding format: GBK
        JavaRDD<String> fileRDD = sc.hadoopFile(filePath, TextInputFormat.class, LongWritable.class, Text.class).map(p -> new String(p._2.getBytes(), 0, p._2.getLength(), "GBK"));
        JavaRDD<event> events = fileRDD.map(event::eventFactory).filter(Objects::nonNull);
        Dataset<event> eventsDataSet = spark.createDataset(events.collect(), Encoders.bean(event.class));
//        eventsDataSet.createTempView("test");
//        spark.sql("SELECT * from test").take(10);
//        events.take(10).stream().forEach(System.out::println);
//        spark.sql("select name from syscolumns where id=object_id('test')");
        eventsDataSet.printSchema();
    }
}
