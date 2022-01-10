package part4sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SparkSql extends App {

  val spark = SparkSession.builder()
    .appName("Spark SQL Practice")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
//    .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // regular DF API
  carsDF.select(col("Name")).where(col("Origin") === "USA")

  // use Spark SQL
  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
    """.stripMargin)

  // we can run ANY SQL statement
  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")
  val databasesDF = spark.sql("show databases")

  // transfer tables from a DB to Spark tables
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  def transferTables(tableNames: List[String], shouldWriteToWarehouse: Boolean = false): Unit = tableNames.foreach { tableName =>
    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)

    if (shouldWriteToWarehouse) {
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }
  }

  transferTables(List(
    "employees",
    "departments",
    "titles",
    "dept_emp",
    "salaries",
    "dept_manager")
  )

  // read DF from warehouse
  val employeesDF2 = spark.read.table("employees")


  /**
    * Exercises
    *
    * 1. Read the movies DF and store it as a Spark table in the rtjvm database.
    * 2. Count how many employees were hired in between Jan 1 1999 and Jan 1 2000.
    * 3. Show the average salaries for the employees hired in between those dates, grouped by department.
    * 4. Show the name of the best-paying department for employees hired in between those dates.
    */

  // 1.
  def transferJsonDFToTable(tableName: String): Unit = {
    val jsonDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/" + tableName + ".json")

      jsonDF.createOrReplaceTempView(tableName)
      jsonDF.write.mode(SaveMode.Overwrite)
      .saveAsTable(tableName)
  }
//  transferJsonDFToTable("movies")

  // 2.
  spark.sql(
    """
      |select count(*)
      |from employees
      |where hire_date > '1999-01-01' and hire_date < '2001-01-01'
      |""".stripMargin
  )//.show()

  // 3.
  spark.sql(
    """
      |select d.dept_name, avg(s.salary)
      |from employees e, salaries s, dept_emp de, departments d
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      |  and e.emp_no = s.emp_no
      |  and e.emp_no = de.emp_no
      |  and de.dept_no = d.dept_no
      |  group by d.dept_name
      |""".stripMargin
  )//.show()

  // 4.
  spark.sql(
    """
      |select d.dept_name, avg(s.salary) payments
      |from employees e, salaries s, dept_emp de, departments d
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      |  and e.emp_no = s.emp_no
      |  and e.emp_no = de.emp_no
      |  and de.dept_no = d.dept_no
      |group by d.dept_name
      |order by payments desc
      |limit 5
      |""".stripMargin
  ).show()

}
