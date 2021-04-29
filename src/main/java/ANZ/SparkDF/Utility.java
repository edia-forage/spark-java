package ANZ.SparkDF;


import java.io.File;
import java.io.FileNotFoundException;

import java.util.ArrayList;
import java.util.Scanner;
import java.util.List;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;


public class Utility{

    public static StructType schemaGenerator(String schemaFilePath)
            throws FileNotFoundException {

        List<StructField> fields = new ArrayList<StructField>();

        try(Scanner sc = new Scanner(new File(schemaFilePath))) {
            while(sc.hasNextLine()){
                StructField field = DataTypes
                        .createStructField(
                                sc.nextLine(),
                                DataTypes.StringType,
                                true);
                fields.add(field);
            }
        }

        return DataTypes.createStructType(fields);

    }
}
