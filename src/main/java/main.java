import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.TextInputFormat;
import scala.Tuple2;

import java.util.regex.Pattern;

public class main {
    public static void main(String[] args) {
        String filePath = "./data.dat";
//        String filePath = "./test.txt";
        SparkConf conf = new SparkConf().setAppName("first").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // read file use encoding format: GBK
        JavaRDD<String> fileRDD = sc.hadoopFile(filePath, TextInputFormat.class, LongWritable.class, Text.class).map(p -> new String(p._2.getBytes(), 0, p._2.getLength(), "GBK"));
        fileRDD = fileRDD.map(s->new String(s.getBytes(),0,s.length(),"gbk"));
//        fileRDD.mapToPair(s -> new Tuple2(s, 1)).reduceByKey((a, b) -> Integer.getInteger(a.toString()) + Integer.getInteger(b.toString())).take(10).stream().forEach(System.out::println);
//        fileRDD.mapToPair(s -> new Tuple2(s, 1)).reduce((a,b)->a).take(10).stream().forEach(System.out::println);
//        fileRDD.map(s -> s.split("\\|")).map(s -> s[0]).take(10).stream().forEach(System.out::println);
        fileRDD.take(10).stream().forEach(System.out::println);
    }
}
