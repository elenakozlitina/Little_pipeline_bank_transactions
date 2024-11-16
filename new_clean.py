
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_timestamp
from py4j.java_gateway import java_import

# Создаем Spark сессию
spark = SparkSession.builder \
    .appName("Data Cleaning for New Files") \
    .getOrCreate()

# Конфигурация Hadoop
java_import(spark._jvm, "org.apache.hadoop.fs.FileSystem")
java_import(spark._jvm, "org.apache.hadoop.fs.Path")
java_import(spark._jvm, "org.apache.hadoop.conf.Configuration")

# Путь к HDFS
input_base_path = "hdfs://your/way/to/data"
output_base_path = "hdfs://your/way/to/clean_data"

# Функция определения директории сохранения по типу файла
def get_output_directory(file_path):
    if "bank_transactions" in file_path:
        return f"{output_base_path}/bank_transactions"
    elif "client_activities" in file_path:
        return f"{output_base_path}/client_activities"
    elif "client_logins" in file_path:
        return f"{output_base_path}/client_logins"
    elif "payments" in file_path:
        return f"{output_base_path}/payments"
    else:
        return f"{output_base_path}/others"

# Функция обработки файлов
def process_new_files():
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.FileSystem.get(hadoop_conf)

    # Список всех папок в директории
    input_path = spark._jvm.Path(input_base_path)
    file_statuses = fs.listStatus(input_path)

    for status in file_statuses:
        if status.isDirectory():
            folder_path = status.getPath().toString()
            csv_files = fs.listStatus(spark._jvm.Path(folder_path))
            csv_file_paths = [f.getPath().toString() for f in csv_files if f.getPath().toString().endswith(".csv")]

            # Если нашли файлы CSV
            if csv_file_paths:
                print(f"Processing files from folder: {folder_path}")
                df = spark.read.csv(csv_file_paths, header=True, inferSchema=True)

                # Универсальная обработка дат
                if "transaction_date" in df.columns:
                    df_cleaned = df.withColumn(
                        "transaction_date",
                        to_timestamp(regexp_replace(col("transaction_date"), "\\.\\d+$", ""), "yyyy-MM-dd HH:mm:ss")
                    )
                elif "activity_date" in df.columns:
                    df_cleaned = df.withColumn(
                        "activity_date",
                        to_timestamp(regexp_replace(col("activity_date"), "\\.\\d+$", ""), "yyyy-MM-dd HH:mm:ss")
                    )
                elif "login_date" in df.columns:
                    df_cleaned = df.withColumn(
                        "login_date",
                        to_timestamp(regexp_replace(col("login_date"), "\\.\\d+$", ""), "yyyy-MM-dd HH:mm:ss")
                    )
                elif "payment_date" in df.columns:
                    df_cleaned = df.withColumn(
                        "payment_date",
                        to_timestamp(regexp_replace(col("payment_date"), "\\.\\d+$", ""), "yyyy-MM-dd HH:mm:ss")
                    )
                else:
                    df_cleaned = df

                # Генерация выходного пути в зависимости от типа файла
                output_path = get_output_directory(folder_path)
                df_cleaned.write.csv(output_path, header=True, mode='overwrite')
                print(f"Saved cleaned data to {output_path}")

                # Удаляем исходные данные после успешной обработки
                fs.delete(spark._jvm.Path(folder_path), True)
                print(f"Deleted processed folder: {folder_path}")

# Остановка Spark сессии
spark.stop()
