from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, when, countDistinct, avg
import os

# Создаем Spark сессию
spark = SparkSession.builder \
    .appName("Anomaly Detection for Clean Data") \
    .config("spark.executor.instances", "2") \
    .config("spark.dynamicAllocation.minExecutors", "2") \
    .getOrCreate()

# Конфигурация базы данных для DBeaver
db_config = {
    'url': 'jdbc:postgresql://host:port/bd',
    'user': 'user',
    'password': '+++',
    'driver': 'org.postgresql.Driver'
}

# Папка для данных в HDFS
base_path = "hdfs://your/path/to/clean_data"

# Функция для сохранения аномалий в базу данных
def save_anomalies_to_db(anomalies_df, table_name):
    anomalies_df.write \
        .format("jdbc") \
        .option("url", db_config['url']) \
        .option("dbtable", table_name) \
        .option("user", db_config['user']) \
        .option("password", db_config['password']) \
        .option("driver", db_config['driver']) \
        .mode("append").save()

# Функции для поиска аномалий
def detect_bank_transactions_anomalies(df, anomaly_threshold=0.3):
    # Вывод имен колонок для отладки
    print("Columns in DataFrame for bank transactions:", df.columns)
    # Вычисляем среднюю сумму переводов по каждому клиенту
    avg_transactions = df.groupBy('client_id').agg(
        avg('amount').alias('avg_transaction_amount')
    )

    # Присоединяем среднюю сумму к исходным данным
    df_with_avg = df.join(avg_transactions, on='client_id', how='left')

    # Определяем аномалии: сумма > 100,000 или отклонение от среднего более чем на 30%
    transaction_anomalies = df_with_avg.filter(
        (col('amount') > 100000) | 
        (col('amount') < col('avg_transaction_amount') * (1 - anomaly_threshold)) | 
        (col('amount') > col('avg_transaction_amount') * (1 + anomaly_threshold))
    ).withColumn(
        'anomaly_type', when(col('amount') > 100000, 'High Transaction Amount')
                        .when(col('amount') < col('avg_transaction_amount') * (1 - anomaly_threshold), 'Low Transaction Amount')
                        .otherwise('High Transaction Amount')
    ).withColumn(
        'anomaly_description', when(col('amount') > 100000, 'Transaction amount exceeds 100,000')
                               .when(col('amount') < col('avg_transaction_amount') * (1 - anomaly_threshold), 
                                     'Transaction amount is significantly lower than average')
                               .otherwise('Transaction amount is significantly higher than average')
    ).select('client_id', 'transaction_date', 'transaction_id', 'amount', 
             'avg_transaction_amount', 'anomaly_type', 'anomaly_description')

    save_anomalies_to_db(transaction_anomalies, 'transaction_anomalies')



def detect_client_activities_anomalies(df):
    # Аномалии: частые действия типа 'transfer_funds'
    activity_anomalies = df.filter(col('activity_type') == 'transfer_funds').groupBy(
        'client_id', 'activity_date'
    ).agg(
        count('activity_type').alias('transfer_count')
    ).filter(
        col('transfer_count') > 5  # Считаем аномалией более 5 переводов в день
    ).withColumn(
        'anomaly_type', when(col('transfer_count') > 5, 'Frequent Transfers')
    ).withColumn(
        'anomaly_description', when(col('transfer_count') > 5, 'More than 5 fund transfers in one day')
    ).select('client_id', 'activity_date', 'transfer_count', 'anomaly_type', 'anomaly_description')

    save_anomalies_to_db(activity_anomalies, 'activity_anomalies')



def detect_client_logins_anomalies(df):
    # Аномалии: использование более 5 различных IP-адресов за день
    login_metrics = df.groupBy('client_id', 'login_date').agg(
        countDistinct('ip_address').alias('distinct_ips')
    )

    login_anomalies = login_metrics.filter(
        col('distinct_ips') > 2  # Считаем аномалией более 2 различных IP за день
    ).withColumn(
        'anomaly_type', when(col('distinct_ips') > 2, 'Suspicious Login Behavior')
    ).withColumn(
        'anomaly_description', when(col('distinct_ips') > 2, 'More than 2 different IPs used in one day')
    ).select('client_id', 'login_date', 'distinct_ips', 'anomaly_type', 'anomaly_description')

    save_anomalies_to_db(login_anomalies, 'login_anomalies')

def detect_payments_anomalies(df, anomaly_threshold=0.3):
    # Вычисляем среднюю сумму платежей по каждому клиенту
    avg_payments = df.groupBy('client_id').agg(
        avg('amount').alias('avg_payment_amount')
    )

    # Присоединяем среднюю сумму к исходным данным
    df_with_avg = df.join(avg_payments, on='client_id', how='left')

    # Определяем аномалии: сумма > 50,000 или отклонение от среднего более чем на 30%
    payment_anomalies = df_with_avg.filter(
        (col('amount') > 50000) | 
        (col('amount') < col('avg_payment_amount') * (1 - anomaly_threshold)) | 
        (col('amount') > col('avg_payment_amount') * (1 + anomaly_threshold))
    ).withColumn(
        'anomaly_type', when(col('amount') > 50000, 'High Payment Amount')
                        .when(col('amount') < col('avg_payment_amount') * (1 - anomaly_threshold), 'Low Payment Amount')
                        .otherwise('High Payment Amount')
    ).withColumn(
        'anomaly_description', when(col('amount') > 50000, 'Payment amount exceeds 50,000')
                               .when(col('amount') < col('avg_payment_amount') * (1 - anomaly_threshold), 
                                     'Payment amount is significantly lower than average')
                               .otherwise('Payment amount is significantly higher than average')
    ).select('client_id', 'payment_date', 'payment_id', 'amount', 
             'avg_payment_amount', 'anomaly_type', 'anomaly_description')

    save_anomalies_to_db(payment_anomalies, 'payment_anomalies')





# Функция для обработки файлов по директориям
def process_files():
    file_types = ['bank_transactions', 'client_activities', 'client_logins', 'payments']

    for file_type in file_types:
        folder_path = f"{base_path}/{file_type}/"

        # Получение списка файлов в директории
        files = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()) \
            .listStatus(spark._jvm.org.apache.hadoop.fs.Path(folder_path))

        total_files = len(files)
        print(f"Начало обработки директории {file_type}. Найдено {total_files} файлов.")
        
        for index, file_status in enumerate(files):
            file_path = file_status.getPath().toString()
            if file_path.endswith(".csv"):
                print(f"Processing file {index + 1} of {total_files}: {file_path}")

                # Загрузка данных
                df = spark.read.csv(file_path, header=True, inferSchema=True)

                # Вызов функции для обнаружения аномалий в зависимости от типа файла
                if file_type == 'bank_transactions':
                    detect_bank_transactions_anomalies(df)
                elif file_type == 'client_activities':
                    detect_client_activities_anomalies(df)
                elif file_type == 'client_logins':
                    detect_client_logins_anomalies(df)
                elif file_type == 'payments':
                    detect_payments_anomalies(df)
                
                # Вывод прогресса
                progress = (index + 1) / total_files * 100
                print(f"Обработка файла завершена: {progress:.2f}%")


# Остановка Spark сессии
spark.stop()
