import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.TextInputFormat;
import scala.Tuple2;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class main {
    public static void main(String[] args) {
        String filePath = "./data.dat";
//        String filePath = "./test.txt";
        SparkConf conf = new SparkConf().setAppName("first").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // read file use encoding format: GBK
        JavaRDD<String> fileRDD = sc.hadoopFile(filePath, TextInputFormat.class, LongWritable.class, Text.class).map(p -> new String(p._2.getBytes(), 0, p._2.getLength(), "GBK"));
//        fileRDD.mapToPair(s -> new Tuple2(s, 1)).reduceByKey((a, b) -> Integer.getInteger(a.toString()) + Integer.getInteger(b.toString())).take(10).stream().forEach(System.out::println);
//        fileRDD.mapToPair(s -> new Tuple2(s, 1)).reduce((a,b)->a).take(10).stream().forEach(System.out::println);
//        fileRDD.map(s -> s.split("\\|")).map(s -> s[0]).take(10).stream().forEach(System.out::println);
        String commonPrefix = "\\d+\\|"; // messageID|
        String baseForm = "\\|(\\w{17})\\|(\\d{15,16})\\|(\\d{17})"; // |随机序列|CA卡号|序列号
        String detailTime = "\\|(\\d{4}\\.\\d{2}\\.\\d{2}\\s\\d{2}:\\d{2}:\\d{2})"; // |yyyy.mm.dd hh:mm:ss
        String baseFormPlus = baseForm + detailTime;
        String recordTime = "\\|\\d{17}.*"; // e.g. |20160502035932193
        String wildcard = "\\|.*";
        String caughtWildcard = "\\|(.*)";
        String openRecord = commonPrefix + "1" + baseFormPlus + wildcard + wildcard + recordTime; // messageID|1|随机序列|CA卡号|序列号|时间|硬件版本号|软件版本号|时间
        String closeRecord = commonPrefix + "2" + baseFormPlus + recordTime; // messageID|2|随机序列|CA卡号|序列号|时间
        String channelQuit = commonPrefix + "5" + baseForm + detailTime + detailTime + wildcard + wildcard + wildcard + caughtWildcard + caughtWildcard + wildcard + wildcard + wildcard + wildcard + "\\|\\d+" + recordTime; // messageID|2|随机序列|CA卡号|序列号|结束时间|开始时间|ServiceID|TSID|频点|频道名称|节目名称|授权|信号强度|信号质量|是否SDV节目|持续时间|时间
        fileRDD.filter(s -> s.matches(closeRecord)).take(10).stream().forEach(System.out::println);
    }
}
