import Event.channelEvent;
import Event.channelQuitEvent;
import Event.event;
import avro.shaded.com.google.common.collect.Lists;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import scala.collection.Seq;

import java.sql.Timestamp;

import java.util.Objects;

import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.desc;

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
        sc.setLogLevel("WARN");
//        readData(spark, sc, filePath);
//        generateSample(spark.read().load("./quit"), 0.00240582402);
//        cleanData(spark);
        getTVRatings(spark.read().load("./channel"), Timestamp.valueOf("2016-5-2 12:00:00"), Timestamp.valueOf("2016-5-2 14:00:00"));
        getUserStatusTransform(spark, "825010214566974", Timestamp.valueOf("2016-5-2 00:00:00"), Timestamp.valueOf("2016-5-2 23:00:00"));
        getWatchTime(spark.read().load("./sample"), "825010214566974", Timestamp.valueOf("2016-5-2 00:00:00"), Timestamp.valueOf("2016-5-2 23:00:00"));
//        spark.read().load("./sample").groupBy("channel", "show").sum("lastTime").sort(desc("sum(lastTime)")).show(100);
//        spark.read().load("./sample").select("CACardID").dropDuplicates("CACardID").sort("CACardID").show(100);

//        getWatchTime(eventsDataSet, "825010304964177", Timestamp.valueOf("2016-1-1 12:00:00"), Timestamp.valueOf("2016-6-1 12:00:00"));
//        getWatchTime(eventsDataSet, "825010385801697", Timestamp.valueOf("2016-1-1 12:00:00"), Timestamp.valueOf("2016-6-1 12:00:00"));
        spark.stop();
    }

    /**
     * @param channelEventsDS the original TV channel data
     * @param startTime       the start time for TV ratings statistics
     * @param endTime         the end time for TV ratings statistics
     */
    private static void getTVRatings(Dataset<Row> channelEventsDS, Timestamp startTime, Timestamp endTime) {
        String timeFilter = "recordTime between '" + startTime.toString() + "' and '" + endTime + "'";
        Dataset<Row> totalUsers = channelEventsDS.where(timeFilter).dropDuplicates("CACardID");
        long userNum = totalUsers.count();
        totalUsers.groupBy("channel", "show").count().selectExpr("channel", "show", "count", "count/" + userNum).sort(desc("count")).show();
    }

    /**
     * @param eventsDS  the event DataSet
     * @param CACardID  the specific user's CA card ID
     * @param startTime the start time for TV ratings statistics
     * @param endTime   the end time for TV ratings statistics
     */
    private static void getWatchTime(Dataset<Row> eventsDS, String CACardID, Timestamp startTime, Timestamp endTime) {
        String timeFilter = "recordTime between '" + startTime.toString() + "' and '" + endTime + "'";
        String CACardIDFilter = "CACardID=" + CACardID;
        eventsDS.where(CACardIDFilter).where(timeFilter).show();
        eventsDS.where(CACardIDFilter).where(timeFilter).groupBy("CACardID").sum("lastTime").sort(desc("sum(lastTime)")).show(100);
    }

    /**
     * show the users' status transform procession order by record time
     *
     * @param sparkSession the spark session
     * @param CACardID     the specific user's CA card ID
     * @param startTime    the start time for TV ratings statistics
     * @param endTime      the end time for TV ratings statistics
     */
    private static void getUserStatusTransform(SparkSession sparkSession, String CACardID, Timestamp startTime, Timestamp endTime) {
        String timeFilter = "recordTime between '" + startTime.toString() + "' and '" + endTime + "'";
        String CACardIDFilter = "CACardID=" + CACardID;
        Dataset<Row> events = sparkSession.read().load("event").where(CACardIDFilter).where(timeFilter);
        Dataset<Row> channel = sparkSession.read().load("channel").where(CACardIDFilter).where(timeFilter);
        Dataset<Row> quit = sparkSession.read().load("quit").where(CACardIDFilter).where(timeFilter);
        Seq<String> joinColumns1 = scala.collection.JavaConversions
                .asScalaBuffer(Lists.newArrayList("CACardID", "recordTime", "typeID"));
        Seq<String> joinColumns2 = scala.collection.JavaConversions
                .asScalaBuffer(Lists.newArrayList("CACardID", "recordTime", "typeID", "channel", "show"));
        events.join(channel, joinColumns1, "outer").join(quit, joinColumns2, "outer").sort("recordTime").show();
    }

    /**
     * generate the specific number of samples for data mining and save as "./sample" table files
     *
     * @param events   the original data in  DataFrame form
     * @param fraction the Sample ratio of all data
     */
    private static void generateSample(Dataset<Row> events, double fraction) {
        Dataset<Row> userSample = events.select("CACardID").dropDuplicates("CACardID").sample(fraction);
        events.join(userSample, events.col("CACardID").equalTo(userSample.col("CACardID")), "leftsemi").sort("CACardID", "recordTime").write().option("path", "./sample").mode(SaveMode.Overwrite).saveAsTable("data1");
    }

    /**
     * clean some impossible data from sample
     *
     * @param sparkSession the spark session
     */
    private static void cleanData(SparkSession sparkSession) {
        Dataset<Row> data = sparkSession.read().load("./sample");
        data.filter("lastTime<86400").write().option("path", "./sample1").mode(SaveMode.Overwrite).saveAsTable("data1");
    }

    /**
     * extract valuable information data from source files and save as three files: './event','./quit','./channel'
     *
     * @param sparkSession     the spark session
     * @param javaSparkContext the spark context in java
     * @param filePath         the file path of the source files
     */
    private static void readData(SparkSession sparkSession, JavaSparkContext javaSparkContext, String filePath) {
        // read file use encoding format: GBK
        JavaRDD<String> fileRDD = javaSparkContext.hadoopFile(filePath, TextInputFormat.class, LongWritable.class, Text.class).map(p -> new String(p._2.getBytes(), 0, p._2.getLength(), "GBK"));
        JavaRDD<event> events = fileRDD.map(event::eventFactory).filter(Objects::nonNull);
        // cache the middle data ,events, to accelerate the procession
        events.persist(StorageLevel.MEMORY_AND_DISK());
        // filter the event type , channelEvent , from events
        JavaRDD<channelEvent> channelEvents = events.filter(s -> s instanceof channelEvent).map(s -> (channelEvent) s);
        // save channel events as sql table file(channel)
        sparkSession.createDataFrame(channelEvents, channelEvent.class).write().option("path", "./channel").mode(SaveMode.Overwrite).saveAsTable("data1");
        // filter the event type , channelQuitEvent , from events
        JavaRDD<channelQuitEvent> channelQuitEvents = events.filter(s -> s instanceof channelQuitEvent).map(s -> (channelQuitEvent) s);
        // save channel quit events as sql table file(quit)
        events.filter(s -> s instanceof channelQuitEvent).map(s -> (channelQuitEvent) s);
        sparkSession.createDataFrame(channelQuitEvents, channelQuitEvent.class).write().option("path", "./quit").mode(SaveMode.Overwrite).saveAsTable("data1");
        // save events as sql table file(event)
        sparkSession.createDataFrame(events, event.class).write().option("path", "./event").mode(SaveMode.Overwrite).saveAsTable("data1");
        events.unpersist();
    }
}
