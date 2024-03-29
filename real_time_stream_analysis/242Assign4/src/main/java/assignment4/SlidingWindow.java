package assignment4;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;

public class SlidingWindow {
    private static String millis = null;
    private static ArrayList<Integer> times = null;
    private static Integer minInterval = null;

    public static void main(String[] args) throws Exception {

        // Create the context with a 120 second batch size
        SparkConf sparkConf = new SparkConf().setAppName("SlidingWindow").setMaster("local[2]");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(120));

        // Create an input stream with the custom receiver on target ip:port and count the
        // words in input stream of \n delimited text (eg. generated by 'nc')
        JavaReceiverInputDStream<String> packets = ssc.receiverStream(
                new CustomReceiverAddr("localhost", Integer.parseInt("9999")));

        //key: string of srcaddr-destaddr, value: time
        JavaPairDStream<String, ArrayList<Integer>> singleaddr = packets.mapToPair(s ->
                new Tuple2<>(s.split(",")[0], Integer.parseInt(s.split(",")[1])))
                .mapToPair(s -> new Tuple2<>(s._1, new ArrayList<>(Arrays.asList(s._2))));

        //key: srcaddr-destaddr, value: array of times
        JavaPairDStream<String, ArrayList<Integer>> addr = singleaddr.reduceByKey((i1, i2) ->
                new ArrayList<>(Arrays.asList(i1.get(0), i2.get(0)))).filter(k -> k._2.size() > 1);
        //addr.print();

        JavaPairDStream<String, ArrayList<Integer>> interval = addr.filter(
                new Function<Tuple2<String, ArrayList<Integer>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, ArrayList<Integer>> stringArrayListTuple2) throws Exception {
                        times = stringArrayListTuple2._2;
                        minInterval = 130;
                        for (int i = 0; i < times.size() - 1; i++) {
                            minInterval = Math.min(minInterval, Math.abs(times.get(i) - times.get(i + 1)));
                        }
                        return minInterval <= 120;
                    }
                }
        );
        //interval.print();


        // store into files
        interval.foreachRDD(rdd -> {
            millis = Long.toString(System.currentTimeMillis());
            rdd.saveAsTextFile("/Users/lizheqing/Downloads/window/w" + millis);
        });

        ssc.start();
        ssc.awaitTermination();
    }

}
