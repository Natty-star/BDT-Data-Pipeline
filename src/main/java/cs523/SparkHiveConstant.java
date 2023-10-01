package cs523;

public class SparkHiveConstant {

    public SparkHiveConstant() {}

    public static final String HIVE_DB = "USE default";
    public static final String LOAD_ALL_RECORDS_SQL = "SELECT * FROM employee";
    public static final String COMPLEX_QUERY = "SELECT * FROM employees limit 20";
    public static final String TABLE_NAME = "employees";
    public static final String WAREHOUSE_DIR = "/home/Natty/Documents/MS-Compro/BDT-FinalProject/apache-hive-3.1.2-bin/warehouse";
    public static final String METADATA_URL = "thrift://localhost:9083";

    public static final String COMPLEX_QUERY_1 = "SELECT * FROM employees limit 20";
    public static final String COMPLEX_QUERY_2 = "SELECT gender, AVG(salary) AS avg_salary FROM employees GROUP BY gender";
    public static final String COMPLEX_QUERY_3 = "SELECT company, AVG(salary) AS avg_salary\n" +        "FROM employees\n" +
            "GROUP BY company\n" +
            "ORDER BY avg_salary DESC\n" +
            "LIMIT 5";
}
