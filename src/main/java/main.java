import Event.channelEvent;
import Event.event;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.sql.Timestamp;
import java.util.List;
import java.util.Objects;

import static org.apache.spark.sql.functions.*;

public class main {
    public static void main(String[] args) throws AnalysisException {
        String filePath = "./data.dat";
        SparkSession spark = SparkSession
                .builder()
                .appName("first")
                .master("local[*]")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        // read file use encoding format: GBK
        JavaRDD<String> fileRDD = sc.hadoopFile(filePath, TextInputFormat.class, LongWritable.class, Text.class).map(p -> new String(p._2.getBytes(), 0, p._2.getLength(), "GBK"));
        JavaRDD<event> events = fileRDD.map(event::eventFactory).filter(Objects::nonNull);
        Dataset<event> eventsDataSet = spark.createDataset(events.collect(), Encoders.bean(event.class));
//        eventsDataSet.createTempView("test");
        JavaRDD<channelEvent> channelEvents = events.filter(s -> s instanceof channelEvent).map(s -> (channelEvent) s);
        Dataset<channelEvent> channelEventsDS = spark.createDataset(channelEvents.collect(), Encoders.bean(channelEvent.class));
//        channelEventsDS.groupBy("channel").count().sort(desc("count")).show();
//        channelEventsDS.groupBy("show").count().sort(desc("count")).show();
//        channelEvents.collect().stream().forEach(System.out::println);
        // compute the TV ratings
//        channelEventsDS.dropDuplicates("CACardID").groupBy("show").count().sort(desc("count")).show();
//        channelEventsDS.first();
//        channelEventsDS.printSchema();
//        eventsDataSet.groupBy("CACardID").count().sort(desc("count")).show();
        Dataset<event> certainChannelDS = eventsDataSet.where("CACardID=825010402320906").sort("recordTime").as(Encoders.bean(event.class));
        System.out.println(certainChannelDS.first().getRecordTime());
//        certainChannelDS.cache();
//        List<event> lists = certainChannelDS.collectAsList();
//        System.out.println(lists.get(lists.size() - 1).getRecordTime().getTime() - lists.get(0).getRecordTime().getTime());
    }
}
