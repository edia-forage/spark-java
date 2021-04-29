package ANZ;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

public class Main {

    public static void main(String[] args) throws Exception{
        System.out.println("Hello to Spark!!!");

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("ANZ-Trans-Source-App")
                .getOrCreate();

        FileUtils.cleanDirectory(new File("output"));

        Instant startTime = Instant.now();
        Helper.dsLoader(new ANZ.SparkDF.ANZTransSource(),
                spark,
                "input/anz_transaction.csv",
                "output/df_trnas.csv");
        System.out.println("Time taken by Dataframe API: "
                + Duration.between(startTime, Instant.now()).toMillis()
                + " in milli seconds");

        startTime = Instant.now();
        Helper.dsLoader(new ANZ.SparkRDD.ANZTransSource(),
                spark,
                "input/anz_transaction.csv",
                "output/rdd_trnas.csv");
        System.out.println("Time taken by RDD API: "
                + Duration.between(startTime, Instant.now()).toMillis()
                + " in milli seconds");

        spark.stop();

        System.out.println("Program ends here!!!");
    }

}

