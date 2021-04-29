package ANZ.SparkRDD;

import org.apache.spark.api.java.function.Function2;

import java.util.Iterator;

public class RemoveHeader implements Function2<Integer, Iterator<String>, Iterator<String>> {

    public Iterator<String> call(Integer index, Iterator<String> iterator)
    {
        if(index==0 && iterator.hasNext())
            iterator.next();

        return iterator;
    }
}
