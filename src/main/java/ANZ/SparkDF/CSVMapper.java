package ANZ.SparkDF;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

class CSVMapper implements Function<String, Row> {

    public Row call(String record) {
        String[] attributes = record.split(",");
        return RowFactory.create(attributes[0], attributes[1].trim());
    }

}
