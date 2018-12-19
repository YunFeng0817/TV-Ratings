import Event.channelEvent;
import Event.channelQuitEvent;
import Event.event;

import java.util.Objects;
import java.sql.Timestamp;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;

import static org.apache.spark.sql.functions.*;

public class main {
    public static void main(String[] args) throws AnalysisException {
        String filePath = "./data";
        SparkSession spark = SparkSession
                .builder()
                .appName("first")
                .master("local[*]")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        readData(spark, sc, filePath);
////        sc.setLogLevel("WARN");
//        // read file use encoding format: GBK
//        JavaRDD<String> fileRDD = sc.hadoopFile(filePath, TextInputFormat.class, LongWritable.class, Text.class).map(p -> new String(p._2.getBytes(), 0, p._2.getLength(), "GBK"));
//        JavaRDD<event> events = fileRDD.map(event::eventFactory).filter(Objects::nonNull);
////        events.persist(StorageLevel.DISK_ONLY());
//        Dataset<event> eventsDataSet = spark.createDataset(events.collect(), Encoders.bean(event.class));
//        System.out.println(eventsDataSet.count());

//        spark.createDataFrame(events, event.class).write().option("path", "./test.table").mode(SaveMode.Overwrite).saveAsTable("test1");
//        Dataset<event> test = spark.read().load("./test.table").as(Encoders.bean(event.class));
//        eventsDataSet = eventsDataSet.union(test);
//        System.out.println(eventsDataSet.count());

//        JavaRDD<channelEvent> channelEvents = events.filter(s -> s instanceof channelEvent).map(s -> (channelEvent) s);
//        Dataset<channelEvent> channelEventsDS = spark.createDataset(channelEvents.collect(), Encoders.bean(channelEvent.class));
//        getTVRatings(channelEventsDS, Timestamp.valueOf("2016-1-1 12:00:00"), Timestamp.valueOf("2016-6-1 12:00:00"));
//        getWatchTime(eventsDataSet, "825010304964177", Timestamp.valueOf("2016-1-1 12:00:00"), Timestamp.valueOf("2016-6-1 12:00:00"));
//        getWatchTime(eventsDataSet, "825010385801697", Timestamp.valueOf("2016-1-1 12:00:00"), Timestamp.valueOf("2016-6-1 12:00:00"));
        spark.stop();
    }

    /**
     * @param channelEventsDS the original TV channel data
     * @param startTime       the start time for TV ratings statistics
     * @param endTime         the end time for TV ratings statistics
     */
    private static void getTVRatings(Dataset<channelEvent> channelEventsDS, Timestamp startTime, Timestamp endTime) {
        String timeFilter = "recordTime between '" + startTime.toString() + "' and '" + endTime + "'";
        Dataset<channelEvent> totalUsers = channelEventsDS.dropDuplicates("CACardID").where(timeFilter);
        long userNum = totalUsers.count();
        totalUsers.groupBy("channel", "show").count().selectExpr("channel", "show", "count", "count/" + userNum).sort(desc("count")).show();
    }

    /**
     * @param eventsDS
     * @param CACardID
     * @param startTime
     * @param endTime
     */
    private static void getWatchTime(Dataset<event> eventsDS, String CACardID, Timestamp startTime, Timestamp endTime) {
        String timeFilter = "recordTime between '" + startTime.toString() + "' and '" + endTime + "'";
        Dataset<event> userRecords = eventsDS.where("CACardID=" + CACardID).where(timeFilter);
        userRecords.show();
    }

    private static void readData(SparkSession sparkSession, JavaSparkContext javaSparkContext, String filePath) {
        // read file use encoding format: GBK
        JavaRDD<String> fileRDD = javaSparkContext.hadoopFile(filePath, TextInputFormat.class, LongWritable.class, Text.class).map(p -> new String(p._2.getBytes(), 0, p._2.getLength(), "GBK"));
        JavaRDD<event> events = fileRDD.map(event::eventFactory).filter(Objects::nonNull);
        events.persist(StorageLevel.MEMORY_AND_DISK());
        JavaRDD<channelEvent> channelEvents = events.filter(s -> s instanceof channelEvent).map(s -> (channelEvent) s);
        sparkSession.createDataFrame(channelEvents, channelEvent.class).write().option("path", "./channel").mode(SaveMode.Overwrite).saveAsTable("data1");
        JavaRDD<channelQuitEvent> channelQuitEvents = events.filter(s -> s instanceof channelQuitEvent).map(s -> (channelQuitEvent) s);
        events.filter(s -> s instanceof channelQuitEvent).map(s -> (channelQuitEvent) s);
        sparkSession.createDataFrame(channelQuitEvents, channelQuitEvent.class).write().option("path", "./quit").mode(SaveMode.Overwrite).saveAsTable("data1");
        sparkSession.createDataFrame(events, event.class).write().option("path", "./event").mode(SaveMode.Overwrite).saveAsTable("data1");
        events.unpersist();
    }
}
