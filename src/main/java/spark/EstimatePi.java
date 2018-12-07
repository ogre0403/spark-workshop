package spark;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class EstimatePi {
    private static Logger logger = Logger.getLogger(EstimatePi.class);

    public static void main(String[] args) {

        int NUM_SAMPLES = 10000;
        SparkConf conf = new SparkConf().setAppName("Pi");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> l = new ArrayList<>(NUM_SAMPLES);
        for (int i = 0; i < NUM_SAMPLES; i++) {
            l.add(i);
        }

        long count = sc.parallelize(l)
                .mapToPair(new PairFunction<Integer, Double, Double>() {
                    @Override
                    public Tuple2<Double, Double> call(Integer integer) throws Exception {
                        return new Tuple2<>(Math.random(), Math.random());
                    }
                })
                .filter(new Function<Tuple2<Double, Double>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Double, Double> v1) throws Exception {
                        return v1._1() * v1._1() + v1._2() * v1._2() < 1;                    }
                })
                .count();

        logger.info("Pi is roughly " + 4.0 * count / NUM_SAMPLES);
        System.out.println("Pi is roughly " + 4.0 * count / NUM_SAMPLES);
    }
}
