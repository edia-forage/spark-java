package ANZ.SparkRDD;

import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SplitColumn implements Function<String, String> {

    Integer[] indexes;
    public SplitColumn(Integer[] fieldPositions){

        this.indexes = fieldPositions;
    }

    public String call(String record)
    {
        List<String> fields = new ArrayList<String>(Arrays.asList(record.split(",")));

        Integer increment = 0;
        for(Integer index: indexes)
        {
            var new_position = index + increment;
            var split_fields = splitField(fields.get(new_position));
            fields.remove(new_position);
            fields.addAll(new_position, Arrays.asList(split_fields));
            increment += 1;
        }

        return String.join(",", fields);
    }

    private String[] splitField(String value){

        String[] fields = new String[2];

        if ( !value.isBlank()){
            String[] values = value.split("\\s+");
            fields[0] = values[0];
            if ( values.length >= 2 ) {
                fields[1] = values[1];
            }
        }

        return fields;
    }
}
