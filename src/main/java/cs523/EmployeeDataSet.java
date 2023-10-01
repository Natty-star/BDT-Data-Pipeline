package cs523;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class EmployeeDataSet {

    public static final String id = "id";
    public static final String first_name = "first_name";
    public static final String last_name = "last_name";
    public static final String email = "email";
    public static final String gender = "gender";
    public static final String job_type = "job_type";
    public static final String salary = "salary";
    public static final String bonus = "bonus";
    public static final String company = "company";
    public static final String city = "city";


    public static Dataset<Row> processEmpDataSet(Dataset<Row> lines) {
        Dataset<Row> schemaDS = lines
                .selectExpr("value",
                        "split(value,',')[0] as id",
                        "split(value,',')[1] as first_name",
                        "split(value,',')[2] as last_name",
                        "split(value,',')[3] as email",
                        "split(value,',')[4] as gender",
                        "split(value,',')[5] as job_type",
                        "split(value,',')[6] as salary",
                        "split(value,',')[7] as bonus",
                        "split(value,',')[8] as company",
                        "split(value,',')[9] as city")
                .drop("value");

        schemaDS = schemaDS

                .withColumn(id, functions.regexp_replace(functions.col(id), " ", ""))
                .withColumn(first_name, functions.regexp_replace(functions.col(first_name), " ", ""))
                .withColumn(last_name, functions.regexp_replace(functions.col(last_name), " ", ""))
                .withColumn(email, functions.regexp_replace(functions.col(email), " ", ""))
                .withColumn(gender, functions.regexp_replace(functions.col(gender), " ", ""))
                .withColumn(job_type, functions.regexp_replace(functions.col(job_type), " ", ""))
                .withColumn(salary, functions.regexp_replace(functions.col(salary), " ", ""))
                .withColumn(bonus, functions.regexp_replace(functions.col(bonus), " ", ""))
                .withColumn(company, functions.regexp_replace(functions.col(company), " ", ""))
                .withColumn(city, functions.regexp_replace(functions.col(city), " ", ""));

        schemaDS = schemaDS
                .withColumn(id, functions.col(id).cast(DataTypes.StringType))
                .withColumn(first_name, functions.col(first_name).cast(DataTypes.StringType))
                .withColumn(last_name, functions.col(last_name).cast(DataTypes.StringType))
                .withColumn(email, functions.col(email).cast(DataTypes.StringType))
                .withColumn(gender, functions.col(gender).cast(DataTypes.StringType))
                .withColumn(job_type, functions.col(job_type).cast(DataTypes.StringType))
                .withColumn(salary, functions.col(salary).cast(DataTypes.StringType))
                .withColumn(bonus, functions.col(bonus).cast(DataTypes.StringType))
                .withColumn(company, functions.col(company).cast(DataTypes.StringType))
                .withColumn(city, functions.col(city).cast(DataTypes.StringType));
        return schemaDS;

    }

}
