/**
 * Illustrates a wordcount in Java
 */
package spark;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCount {
    private static Logger logger = Logger.getLogger(WordCount.class);

    public static void main(String[] args) throws Exception {
        String inputFile = args[0];

        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load our input data.
        JavaRDD<String> input = sc.textFile(inputFile);

        // Split up into words.
        JavaRDD<String> words = input
            .flatMap(new FlatMapFunction<String, String>() {
                public Iterable<String> call(String x) {
                    return Arrays.asList(x.split(" "));
                }
            });

        // Transform into <word, one> pair.
        JavaPairRDD<String, Integer> word_one = words
            .mapToPair(new PairFunction<String, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(String s) throws Exception {
                    return new Tuple2<>(s, 1);
                }
            }).cache();

        List<Tuple2<String, Integer>> result;

        JavaPairRDD<String, Integer> counts_apporache_1 = word_one
                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                });
        result = counts_apporache_1.collect();
        for(Tuple2 r : result)
            logger.info(r);

        JavaPairRDD<String, Integer> counts_apporache_2 = word_one
            .foldByKey(0, new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return v1 + v2;
                }
            });
        result = counts_apporache_2.collect();
        for(Tuple2 r : result)
            logger.info(r);

        JavaPairRDD<String, Integer> counts_apporache_3 = word_one
            .groupByKey()
            .mapValues(new Function<Iterable<Integer>, Integer>() {
                @Override
                public Integer call(Iterable<Integer> v1) throws Exception {
                    int sum = 0;
                    for (Integer v : v1)
                        sum = sum + v;
                    return sum;
                }
            });
        result = counts_apporache_3.collect();
        for(Tuple2 r : result)
            logger.info(r);



    }
}
