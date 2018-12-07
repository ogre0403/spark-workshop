package spark;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by ogre0403 on 2016/2/11.
 */

//TODO: Exercise iii-5
public class complexJob {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ComplexJob test");
        JavaSparkContext sc = new JavaSparkContext(conf);


        List<Tuple2<Integer, Character>> data1 = new LinkedList<>();
        data1.add(new Tuple2<>(1,'a'));
        data1.add(new Tuple2<>(2,'b'));
        data1.add(new Tuple2<>(3,'c'));
        data1.add(new Tuple2<>(4,'d'));
        data1.add(new Tuple2<>(5,'e'));
        data1.add(new Tuple2<>(3,'f'));
        data1.add(new Tuple2<>(2,'g'));
        data1.add(new Tuple2<>(1, 'h'));




        JavaPairRDD<Integer, Character> rangePairs1 = sc.parallelizePairs(data1, 3);

        JavaPairRDD hashPairs1 = rangePairs1.partitionBy(new HashPartitioner(3));


        List<Tuple2<Integer, String>> data2 = new LinkedList<>();
        data2.add(new Tuple2<>(1,"A"));
        data2.add(new Tuple2<>(2,"B"));
        data2.add(new Tuple2<>(3,"C"));
        data2.add(new Tuple2<>(4, "D"));


        JavaPairRDD<Integer, String> pairs2 = sc.parallelizePairs(data2, 2);

        JavaPairRDD<Integer, Character> rangePairs2 = pairs2.mapToPair(x -> new Tuple2<>(x._1(), x._2().charAt(0)));


        List<Tuple2<Integer, Character>> data3 = new LinkedList<>();
        data3.add(new Tuple2<>(1,'X'));
        data3.add(new Tuple2<>(2,'Y'));
        JavaPairRDD<Integer, Character> rangePairs3 = sc.parallelizePairs(data3, 2);

        JavaPairRDD<Integer, Character> rangePairs = rangePairs2.union(rangePairs3);
        JavaPairRDD result = hashPairs1.join(rangePairs);

        result.collect();


    }
}
