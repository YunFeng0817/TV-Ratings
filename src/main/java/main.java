import Event.channelEvent;
import Event.channelQuitEvent;
import Event.event;
import avro.shaded.com.google.common.collect.Lists;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.regression.*;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import scala.collection.Seq;

import java.sql.Timestamp;

import java.util.Objects;

import static org.apache.spark.sql.functions.*;

public class main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("first")
                .master("local[*]")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        sc.setLogLevel("WARN");
//        readData(spark, sc, "./data");
//        generateSample(spark.read().load("./quit"), 0.00240582402);
//        generateTVRatings(spark.read().load("./quit"), Timestamp.valueOf("2016-5-2 00:00:00"), Timestamp.valueOf("2016-5-2 23:00:00"));
//        cleanData(spark);
        getTVRatings(spark.read().load("./channel"), Timestamp.valueOf("2016-5-2 12:00:00"), Timestamp.valueOf("2016-5-2 14:00:00"));
        getUserStatusTransform(spark, "825010214566974", Timestamp.valueOf("2016-5-2 00:00:00"), Timestamp.valueOf("2016-5-2 23:00:00"));
        getWatchTime(spark.read().load("./sample"), "825010385941182", Timestamp.valueOf("2016-5-2 00:00:00"), Timestamp.valueOf("2016-5-2 23:00:00"));
//        generateRatingPrediction(spark);
        ratingPrediction(spark);
//        generatePreferenceData(spark);
        userPreferencePredict(spark, "825010226885922");
        spark.stop();
    }

    /**
     * show the top 20 TV rating data
     *
     * @param channelEventsDS the original TV channel data
     * @param startTime       the start time for TV ratings statistics
     * @param endTime         the end time for TV ratings statistics
     */
    private static void getTVRatings(Dataset<Row> channelEventsDS, Timestamp startTime, Timestamp endTime) {
        String timeFilter = "recordTime between '" + startTime.toString() + "' and '" + endTime + "'";
        Dataset<Row> totalUsers = channelEventsDS.where(timeFilter).dropDuplicates("CACardID");
        totalUsers.cache();
        long userNum = totalUsers.count();
        totalUsers.groupBy("channel", "show").count().selectExpr("channel", "show", "count", "count/" + userNum).toDF("channel", "show", "count", "rating").sort(desc("count")).show();
        totalUsers.unpersist();
    }

    /**
     * show the top 100 users' watching time
     *
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
     * generate training data for rating prediction
     *
     * @param sparkSession the spark session
     */
    private static void generateRatingPrediction(SparkSession sparkSession) {
        // generate source data file for training and testing
//        Dataset<Row> channel = sparkSession.read().load("./rating").filter("NOT( (channel = '')OR (show = 'NULL')OR (show = '')OR (show = 'null')OR (show = '以播出为准'))").filter("count>20").select("channel", "show", "count");
//        channel.cache();
//        Dataset<Row> channelProperty = sparkSession.read().load("./quit").filter("NOT channel = ''").filter("lastTime>60").groupBy("channel").count().selectExpr("channel", "count", "count-1").toDF("channel", "count", "temp").selectExpr("channel", "temp/202642").toDF("channel", "weight");
//        Dataset<Row> showProperty = sparkSession.read().load("./channel").filter("typeID=21").selectExpr("show", "hour(recordTime)").groupBy("show").agg(functions.min(col("hour(recordTime)"))).toDF("show", "startHour");
//        channel.join(channelProperty, scala.collection.JavaConversions
//                .asScalaBuffer(Lists.newArrayList("channel")), "left_outer").join(showProperty, scala.collection.JavaConversions
//                .asScalaBuffer(Lists.newArrayList("show")), "left_outer").sample(0.1).limit(100).write().option("path", "./source").mode(SaveMode.Overwrite).saveAsTable("data1");
//        channel.join(channelProperty, scala.collection.JavaConversions
//                .asScalaBuffer(Lists.newArrayList("channel")), "left_outer").join(showProperty, scala.collection.JavaConversions
//                .asScalaBuffer(Lists.newArrayList("show")), "left_outer").sample(0.1).limit(50).write().option("path", "./testSource").mode(SaveMode.Overwrite).saveAsTable("data1");

//        sparkSession.read().load("./source").select("show").dropDuplicates("show").as(Encoders.STRING()).collectAsList().forEach(System.out::println);
        // generate training and testing data set and write as train and test table file
        Dataset<Row> showType = sparkSession.read().csv("./showtype.csv").toDF("show", "type").withColumn("type", col("type").cast("int"));
        Dataset<Row> data = sparkSession.read().load("./source");
        data.join(showType, scala.collection.JavaConversions
                .asScalaBuffer(Lists.newArrayList("show")), "left_outer").filter("NOT type is null").select("startHour", "type", "weight", "count").write().option("path", "./train").mode(SaveMode.Overwrite).saveAsTable("data1");
        data = sparkSession.read().load("./testSource");
        data.join(showType, scala.collection.JavaConversions
                .asScalaBuffer(Lists.newArrayList("show")), "left_outer").filter("NOT type is null").select("startHour", "type", "weight", "count").write().option("path", "./test").mode(SaveMode.Overwrite).saveAsTable("data1");
    }

    /**
     * use linear regression method to predict the TV rating
     *
     * @param sparkSession the spark session
     */
    private static void ratingPrediction(SparkSession sparkSession) {
        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(new String[]{"startHour", "type", "weight"})
                .setOutputCol("features");
        Dataset<Row> training = vectorAssembler.transform(sparkSession.read().load("./train"));
        Dataset<Row> testing = vectorAssembler.transform(sparkSession.read().load("./test"));
        LinearRegressionModel lrModel = new LinearRegression()
                .setMaxIter(10000)
                .setRegParam(0)
                .setElasticNetParam(0.8)
                .setFeaturesCol("features")
                .setLabelCol("count")
                .fit(training);
        // Fit the model.
        Dataset<Row> predictions = lrModel.transform(testing);
        System.out.println("Use linear regression method result");
        predictions.show(100);
        RegressionEvaluator regressionEvaluator = new RegressionEvaluator()
                .setLabelCol("count")
                .setPredictionCol("prediction")
                .setMetricName("rmse");

        System.out.println("Coefficients: " + lrModel.coefficients() + " Intercept: " + lrModel.intercept());
        // Summarize the model over the training set and print out some metrics.
        LinearRegressionTrainingSummary trainingSummary = lrModel.summary();
        System.out.println("numIterations: " + trainingSummary.totalIterations());
        double rmse = regressionEvaluator.evaluate(predictions);
        System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);

        // Train a DecisionTree model.
        System.out.println("Use decision tree regression method result");
        DecisionTreeRegressionModel dt = new DecisionTreeRegressor()
                .setFeaturesCol("features")
                .setLabelCol("count")
                .fit(training);
        // Fit the model.
        predictions = dt.transform(testing);
        predictions.show(100);
        // Select (prediction, true label) and compute test error.
        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setLabelCol("count")
                .setPredictionCol("prediction")
                .setMetricName("rmse");
        rmse = evaluator.evaluate(predictions);
        System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);

        // Train a RandomForest model.
        System.out.println("Use random forest regression method result");
        RandomForestRegressionModel randomForestRegressionModel = new RandomForestRegressor()
                .setLabelCol("count")
                .setFeaturesCol("features")
                .fit(training);
        predictions = randomForestRegressionModel.transform(testing);
        predictions.show(100);
        evaluator = new RegressionEvaluator()
                .setLabelCol("count")
                .setPredictionCol("prediction")
                .setMetricName("rmse");
        rmse = evaluator.evaluate(predictions);
        System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);

        // Train a GBT model.
        System.out.println("Use gradient-boosted tree regression method result");
        GBTRegressor gbt = new GBTRegressor()
                .setLabelCol("count")
                .setFeaturesCol("features")
                .setMaxIter(100);
        predictions = gbt.fit(training).transform(testing);
        predictions.show(100);
        // Select (prediction, true label) and compute test error.
        evaluator = new RegressionEvaluator()
                .setLabelCol("count")
                .setPredictionCol("prediction")
                .setMetricName("rmse");
        rmse = evaluator.evaluate(predictions);
        System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);
    }

    private static void userPreferencePredict(SparkSession sparkSession, String CACardID) {
        // transform the DataSet to vector for machine learning
        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(new String[]{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15"})
                .setOutputCol("features");
        Dataset<Row> data = sparkSession.read().load("users");
        Dataset<Row> quit = sparkSession.read().load("quit");
        Dataset<Row> showType = sparkSession.read().csv("./showtype.csv").toDF("show", "type").withColumn("type", col("type").cast("int"));
        data = vectorAssembler.transform(data);
        // Trains a k-means model.
        KMeans kmeans = new KMeans().setK(6).setSeed(1L);
        KMeansModel model = kmeans.fit(data).setFeaturesCol("features");
        // Shows the result.
        System.out.println("Cluster Centers: ");
        Vector[] centers = model.clusterCenters();
        for (Vector center : centers) {
            System.out.println(center);
        }
        Dataset<Row> prediction = model.transform(data);
        Dataset<Row> one = quit.filter("lastTime < 43200").where("CACardID=" + CACardID).join(showType, scala.collection.JavaConversions
                .asScalaBuffer(Lists.newArrayList("show")), "outer").filter("NOT type is null").groupBy("CACardID").pivot("type").sum("lastTime").where("CACardID=" + CACardID).na().fill(0, new String[]{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15"});
        one = vectorAssembler.transform(one);
        int type = model.transform(one).select("prediction").as(Encoders.INT()).head();
        Dataset<Row> sameTypeUsers = prediction.where("prediction=" + type).select("CACardID");
        System.out.println("\n\nThe channels and shows recommended for user: " + CACardID);
        quit.join(sameTypeUsers, quit.col("CACardID").equalTo(sameTypeUsers.col("CACardID")), "left_semi").groupBy("channel", "show").count().orderBy(desc("count")).show();
    }

    /**
     * generate source data for training users' viewing preferences
     *
     * @param sparkSession the spark session
     */
    private static void generatePreferenceData(SparkSession sparkSession) {
        Dataset<Row> quit = sparkSession.read().load("quit");
        Dataset<Row> showType = sparkSession.read().csv("./showtype.csv").toDF("show", "type").withColumn("type", col("type").cast("int"));
        Dataset<Row> user = quit.join(showType, scala.collection.JavaConversions
                .asScalaBuffer(Lists.newArrayList("show")), "left_outer").filter("NOT type is null").groupBy("CACardID").count().sort(desc("count")).limit(500);
        quit.filter("lastTime < 43200").join(user, quit.col("CACardID").equalTo(user.col("CACardID")), "left_semi").join(showType, scala.collection.JavaConversions
                .asScalaBuffer(Lists.newArrayList("show")), "left_outer").filter("NOT type is null").groupBy("CACardID").pivot("type").sum("lastTime").na().fill(0, new String[]{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15"}).write().option("path", "./users").saveAsTable("table1");
    }

    /**
     * generate rating data for TV rating prediction
     *
     * @param channelEventsDS the original TV channel data
     * @param startTime       the start time for TV ratings statistics
     * @param endTime         the end time for TV ratings statistics
     */
    private static void generateTVRatings(Dataset<Row> channelEventsDS, Timestamp startTime, Timestamp endTime) {
        String timeFilter = "recordTime between '" + startTime.toString() + "' and '" + endTime + "'";
        Dataset<Row> totalUsers = channelEventsDS.where(timeFilter).dropDuplicates("CACardID");
        totalUsers.cache();
        long userNum = totalUsers.count();
        totalUsers.groupBy("channel", "show").count().selectExpr("channel", "show", "count", "count/" + userNum).toDF("channel", "show", "count", "rating").write().option("path", "./rating").mode(SaveMode.Overwrite).saveAsTable("data1");
        totalUsers.unpersist();
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
