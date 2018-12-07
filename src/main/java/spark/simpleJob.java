package spark;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by ogre0403 on 2016/2/11.
 */

//TODO: Exercise iii-5
public class simpleJob {
    private static Logger logger = Logger.getLogger(simpleJob.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SimpleJob test");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> raw = new LinkedList<>();
        raw.add("A");
        raw.add("B");
        raw.add("C");
        raw.add("A");
        raw.add("B");

        JavaPairRDD<String, Integer> kv = sc.parallelize(raw).mapToPair(
                s -> new Tuple2<>(s,1)
        ).cache();

        long count  = kv.count();

        logger.info("First action count() result : " + count);

        kv.groupByKey().partitions().size();
        Map<String, Object> r = kv.groupByKey().countByKey();

        logger.info("Second action countByKey() result :");
        for (Map.Entry<String, Object> entry : r.entrySet()){
            logger.info(entry.getKey() + " / " + entry.getValue());
        }


    }
}
