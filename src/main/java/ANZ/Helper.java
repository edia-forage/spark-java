package ANZ;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

class Helper {

    public static boolean dsLoader(AnySource source, SparkSession spark, String inputPath, String outputPath)
            throws Exception {
        return source.process(spark, inputPath, outputPath);
    }
}
