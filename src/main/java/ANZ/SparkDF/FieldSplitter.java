package ANZ.SparkDF;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Function1;

import static org.apache.spark.sql.functions.split;

class FieldSplitter implements Function1<Dataset<Row>, Dataset<Row>> {

    String[] splitFields;

    public FieldSplitter(String[] fields) {
        splitFields = fields;
    }


    @Override
    public Dataset<Row> apply(Dataset<Row> records) {

        for (String field : splitFields) {
            records.withColumn(field + "_1",
                    split(records.col(field), " ").getItem(0))
                    .withColumn(field + "_2",
                            split(records.col(field), " ").getItem(1));
        }

        return records;
    }
}
