package ANZ.SparkDF;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

import java.io.FileNotFoundException;
import org.apache.spark.api.java.function.Function;
import scala.Function1;

public class ANZTransSource implements ANZ.AnySource{

    public boolean process(SparkSession spark, String inputPath, String outputPath)
    {

        Dataset<Row> transData = spark.read().format("csv")
                .option("sep",",")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(inputPath);

        //StructType schema = Utility.schemaGenerator("schemas/columns.txt");

        String[] fields = new String[]{"long_lat", "merchant_long_lat"};

        Dataset<Row> transformed_rows = transData
                .filter((FilterFunction<Row>) r -> r.getAs("status").equals("authorized"))
                .filter((FilterFunction<Row>) r -> r.getAs("card_present_flag").equals(0))
                .transform(new FieldSplitter(fields));

        System.out.println(transformed_rows.count());

        transformed_rows.write().csv(outputPath);

        return true;
    }
}


