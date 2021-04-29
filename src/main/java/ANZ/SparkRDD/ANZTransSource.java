package ANZ.SparkRDD;

import ANZ.AnySource;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class ANZTransSource implements AnySource {

    public boolean process(SparkSession spark, String inputPath, String outputPath){

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> data = sc.textFile("input/anz_transaction.csv", 2);


        JavaRDD<String> transactions = data
                .filter(rec -> !(null == rec))
                .filter(rec -> !(rec.length() == 0))
                .mapPartitionsWithIndex(new ANZ.SparkRDD.RemoveHeader(),
                        false
                );

        JavaRDD<String> filtered_trans = transactions.filter(new ANZ.SparkRDD.AuthorizedFilter());

        Integer[] fieldPositions = {5, 21};

        JavaRDD<String> modified_trans = filtered_trans.map(new SplitColumn(fieldPositions));

        System.out.println(modified_trans.count());

        modified_trans.saveAsTextFile(outputPath);

        sc.stop();

        return true;

    }
}
