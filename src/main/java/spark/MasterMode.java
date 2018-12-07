package spark;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by ogre0403 on 2016/2/10.
 */
public class MasterMode {

    private static Logger logger = Logger.getLogger(MasterMode.class);


    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("MasterMode");
        JavaSparkContext sc = new JavaSparkContext(conf);

        logger.info("AppName: " + sc.appName());
        logger.info("isLocal: " + sc.isLocal());
        logger.info("Master: " + sc.master());

    }
}
