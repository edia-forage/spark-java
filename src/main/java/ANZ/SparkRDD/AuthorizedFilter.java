package ANZ.SparkRDD;

import org.apache.spark.api.java.function.Function;

public class AuthorizedFilter implements Function<String, Boolean> {

    public Boolean call(String record)
    {
        String[] data_elements = record.split(",");

        if ( !data_elements[0].isBlank()
                && data_elements[0].toUpperCase().equals("AUTHORIZED") )
            if( !data_elements[1].isBlank() && data_elements[1].toUpperCase().equals("0"))
                return true;

        return false;
    }
}