import logging
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    when,
    length,
    col,
    unix_timestamp,
    date_format,
    sum,
    collect_list,
    explode,
    substring,
)
from pyspark.sql.types import (
    StringType,
    IntegerType,
    DateType,
    DecimalType,
)


# Access the configuration file using SparkFiles
config_file_path = "/opt/spark/conf/spark-config.conf"
# Read secrete from config
config = configparser.ConfigParser()
config.read(config_file_path)

# Logging here
log_filename = "pyspark_etl.log"
log_format = "%(asctime)s [%(levelname)s] - %(message)s"
logging.basicConfig(
    filename=log_filename, level=logging.INFO, format=log_format
)


# Start Spark Sesssion here
def create_spark_session():
    return SparkSession.builder.appName("ETL.com").getOrCreate()


# Defind a function to read from the MySQL database
def read_from_mysql(spark, table_name):
    dbDriver = config.get("db", "driver")
    dbUrl = config.get("db", "url")
    dbUsername = config.get("db", "username")
    dbPassword = config.get("db", "password")
    return (
        spark.read.format("jdbc")
        .option("driver", dbDriver)
        .option("url", dbUrl)
        .option("dbtable", table_name)
        .option("user", dbUsername)
        .option("password", dbPassword)
        .load()
    )


# Define the timestamp data to hour
def calculate_to_hour(df):
    # Convert to the unix time to calculate the hour
    df = df.withColumn(
        "unix_start", unix_timestamp(col("start_time"), "yyyy-MM-dd HH:mm:ss")
    ).withColumn(
        "unix_end", unix_timestamp(col("end_time"), "yyyy-MM-dd HH:mm:ss")
    )

    # Calculate the time delta
    df = df.withColumn("time_difference", col("unix_end") - col("unix_start"))

    # Convert unix time to hour
    df = df.withColumn("in_hour", col("time_difference") / 3600)
    return df


# Define a function to calculate utilization percentages
def calculate_utilization_percentage(column_name):
    result = (col(column_name) / col("available_utilization_hour")) * 100
    logging.info("Calculated utilization percentage: %s", result)
    return result


def vehicle_status_calculator(df, status_alias_mapping):
    # Create a list of 'when' expressions and aliases using a loop
    column_expressions = [
        when(df["status"] == status, df["in_hour"]).otherwise(0).alias(alias)
        for status, alias in status_alias_mapping.items()
    ]

    # Add the "available_utilization_hour" column using the 'isin' condition
    column_expressions.insert(
        0,
        when(
            df["status"].isin(list(status_alias_mapping.keys())), df["in_hour"]
        )
        .otherwise(0)
        .alias("available_utilization_hour"),
    )

    # Select the columns using the generated expressions
    result = df.select("vehicle_id", *column_expressions)

    # Group by 'vehicle_id' and calculate the sums
    aggregated_columns = [
        sum(alias).alias(alias) for alias in status_alias_mapping.values()
    ] + [sum("available_utilization_hour").alias("available_utilization_hour")]
    result = result.groupBy("vehicle_id").agg(*aggregated_columns)

    # Calculate utilization percentages using the function
    logging.info("Calculate utilization percentages.")
    result = (
        result.withColumn(
            "utilization",
            calculate_utilization_percentage("on_rent")
            + calculate_utilization_percentage("replacement")
            + calculate_utilization_percentage("overdue"),
        )
        .withColumn(
            "drive_car_utilization",
            calculate_utilization_percentage("drive_car"),
        )
        .withColumn(
            "ops_utilization",
            calculate_utilization_percentage("cleaning_process")
            + calculate_utilization_percentage("relocation")
            + calculate_utilization_percentage("transit")
            + calculate_utilization_percentage("panel_shop")
            + calculate_utilization_percentage("maintenance")
            + calculate_utilization_percentage("pending"),
        )
        .withColumn(
            "lost_utilization", calculate_utilization_percentage("idle_in_yard")
        )
    )

    result = result.withColumnRenamed("vehicle_id", "result_vehicle_id")
    return result


def vehicle_daily_utilization(df):
    df = df.withColumn("created_date", date_format("created_at", "yyyy-MM-dd"))
    # Group by 'vehicle_id' and collect unique 'created_date' values into a list
    unique_created_dates = df.groupBy("vehicle_id").agg(
        collect_list("created_date").alias("distinct_created_dates")
    )

    # Explode the list into separate rows
    unique_created_dates = unique_created_dates.withColumn(
        "distinct_created_date", explode("distinct_created_dates")
    ).drop("distinct_created_dates")

    # Get distinct, non-duplicated 'created_date' values
    unique_created_dates = unique_created_dates.select(
        "vehicle_id", "distinct_created_date"
    ).distinct()

    # change the column name vehicle_id to date_vehicle_id
    unique_created_dates = unique_created_dates.withColumnRenamed(
        "vehicle_id", "date_vehicle_id"
    )
    return unique_created_dates


def data_type_casting(df):
    df = df.withColumn("created_date", col("created_date").cast(DateType()))
    df = df.withColumn("vehicle_id", col("vehicle_id").cast(IntegerType()))

    # Columns to cast to DecimalType(10, 4)
    columns_to_cast = [
        "idle_in_yard",
        "on_rent",
        "replacement",
        "drive_car",
        "cleaning_process",
        "relocation",
        "transit",
        "panel_shop",
        "maintenance",
        "pending",
        "overdue",
    ]

    # Cast the specified columns to DecimalType(10,4)
    for column in columns_to_cast:
        df = df.withColumn(column, col(column).cast(DecimalType(10, 4)))

    # Define the maximum length
    max_length = 15

    # Columns to apply the constraint
    columns_to_constrain = [
        "utilization",
        "drive_car_utilization",
        "ops_utilization",
        "lost_utilization",
    ]
    for column in columns_to_constrain:
        df = df.withColumn(
            column,
            when(
                length(col(column)) > max_length,
                substring(col(column), 1, max_length),
            ).otherwise(col(column)),
        )

    # Cast to StringType
    for column in columns_to_constrain:
        df = df.withColumn(column, col(column).cast(StringType()))

    return df


def load_data_to_parquet(df):
    try:
        df.write.mode("overwrite").parquet("daily_vehicle_usage")
        logging.info("Parquet file write success")
        return True
    except Exception as e:
        logging.error("Parquet file write error: %s.", str(e))
        return False


def write_data_to_sql(df, tablename):
    dbDriver = config.get("db", "driver")
    dbUrl = config.get("db", "url")
    dbUsername = config.get("db", "username")
    dbPassword = config.get("db", "password")
    try:
        df.write.mode("append").format("jdbc").option(
            "driver", dbDriver
        ).option("url", dbUrl).option("dbtable", tablename).option(
            "user", dbUsername
        ).option(
            "password", dbPassword
        ).save()
        logging.info("Save the Data to the SQL server")
        return True
    except Exception as e:
        logging.error("Data inserting error %s.", str(e))
        return False


def main():
    """
    Extract data from the MySQL database tables
    Transfrom timestamp to
        1) hour
        2) percentage
        3) group by vehicle id and date to get daily usage
    Load data to parquet file and SQL server
    """
    logging.info("----------------------------")
    logging.info("ETL script started.")
    logging.info("----------------------------")
    logging.info("Spark Session started.")
    spark = create_spark_session()

    # Extract start here
    logging.info("--Start Extract Data From tables.")
    logging.info("----------------------------")
    try:
        logging.info("Read from vehicles tables.")
        df_vehicles = read_from_mysql(spark, "vehicles")
    except Exception as e:
        logging.error("An error occurred: %s", str(e))

    try:
        logging.info("Read from vehicle_histories tables.")
        df_vehicle_histories = read_from_mysql(spark, "vehicle_histories")
    except Exception as e:
        logging.error("An error occurred: %s", str(e))

    try:
        logging.info("Read from locations tables.")
        df_locations = read_from_mysql(spark, "locations")
    except Exception as e:
        logging.error("An error occurred: %s", str(e))

    logging.info("--End Extract Data From tables.")

    # Convert to hour
    logging.info("--Start Transformation Data.")
    logging.info("----------------------------")
    logging.info("Start converting to vehicles time delta to hour function.")
    df_vehicle_histories = calculate_to_hour(df_vehicle_histories)
    logging.info("End converting to vehicles time delta to hour function.")

    # Define the list of statuses and corresponding aliases in a dictionary
    status_alias_mapping = {
        "YARD": "idle_in_yard",
        "ONRENT": "on_rent",
        "CLEANING": "cleaning_process",
        "REPLACEMENT": "replacement",
        "DRIVE_CAR": "drive_car",
        "RELOCATION": "relocation",
        "TRANSIT": "transit",
        "PANELSHOP": "panel_shop",
        "MAINTENANCE": "maintenance",
        "PENDING": "pending",
        "OVERDUE": "overdue",
    }
    logging.info("Start vehical status calculator function.")
    # vehicle status calculator
    JoinDF1 = vehicle_status_calculator(
        df_vehicle_histories, status_alias_mapping
    )
    logging.info("End vehical status calculator function.")

    logging.info("Start daily_utilization function.")
    # daily vehicle utilization
    JoinDF2 = vehicle_daily_utilization(df_vehicle_histories)
    logging.info("End daily_utilization function.")

    logging.info("Start Join Two DataFrames JoinDF1 and JoinDF2")
    # get vehicle history data
    JoinVehicleHistory = JoinDF1.join(
        JoinDF2,
        JoinDF1["result_vehicle_id"] == JoinDF2["date_vehicle_id"],
        "inner",
    )
    JoinVehicleHistory = JoinVehicleHistory.withColumnRenamed(
        "distinct_created_date", "created_date"
    )
    logging.info("End Join Two DataFrames JoinDF1 and JoinDF2")

    logging.info("Start Join JoinVehicleHistory and df_vehicles: info")
    # vehicle history and vehicle info
    vehicle_cols = [
        "vehicle_id",
        "vehicle_name",
        "license_plate_number",
        "location_id",
    ]
    Join_vehicles_data = df_vehicles[vehicle_cols].join(
        JoinVehicleHistory,
        df_vehicles["vehicle_id"] == JoinVehicleHistory["result_vehicle_id"],
        "inner",
    )
    logging.info("End Join JoinVehicleHistory and df_vehicles: info")

    # Add location_name to the final report data frame
    logging.info("Start Join Join_vehicles_data and vehicle location")
    # To get the Vehicle location
    final_result = Join_vehicles_data.join(
        df_locations[["location_id", "location_name"]],
        Join_vehicles_data["location_id"] == df_locations["location_id"],
        "inner",
    )

    logging.info("End Join Join_vehicles_data and vehicle location")
    logging.info("Start Type Casting.")
    final_report = final_result.select(
        "created_date",
        "vehicle_id",
        "vehicle_name",
        "license_plate_number",
        "location_name",
        "available_utilization_hour",
        "idle_in_yard",
        "on_rent",
        "replacement",
        "drive_car",
        "cleaning_process",
        "relocation",
        "transit",
        "panel_shop",
        "maintenance",
        "pending",
        "overdue",
        "utilization",
        "drive_car_utilization",
        "ops_utilization",
        "lost_utilization",
    )

    final_report = data_type_casting(final_report)
    logging.info("End Type Casting.")
    logging.info("--End Transformation Data.")
    logging.info("----------------------------")
    logging.info("--Start Loading Data.")
    try:
        logging.info("Save report Format as parquet")
        load_data_to_parquet(final_report)
        logging.info("Save report Format to warehouse")
        write_data_to_sql(final_report, "report")
    except Exception as e:
        logging.error("An error occurred: %s", str(e))
    logging.info("--End Loading Data.")
    spark.stop()


if __name__ == "__main__":
    main()
