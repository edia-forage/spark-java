package ANZ;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

public interface AnySource {
    public boolean process(SparkSession spark, String inputPath, String outputPath);
}
