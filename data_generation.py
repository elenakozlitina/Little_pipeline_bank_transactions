import random
import time
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import Row
from faker import Faker

# Инициализация сессии Spark
spark = SparkSession.builder \
    .appName("Data Generation") \
    .getOrCreate()

fake = Faker()

# Параметры генерации данных
max_records_per_file = 1000

def random_date(start, end):
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

start_date = datetime.now() - timedelta(days=365)
end_date = datetime.now()

def generate_login_data(client_id):
    login_date = random_date(start_date, end_date)
    return Row(
        client_id=client_id,
        login_date=login_date,
        ip_address=fake.ipv4(),
        location=f"{random.uniform(-90, 90)}, {random.uniform(-180, 180)}",
        device=fake.user_agent()
    )

def generate_activity_data(client_id):
    activity_date = random_date(start_date, end_date)
    return Row(
        client_id=client_id,
        activity_date=activity_date,
        activity_type=random.choice(['view_account', 'transfer_funds', 'pay_bill', 'login', 'logout']),
        activity_location=fake.uri_path(),
        ip_address=fake.ipv4(),
        device=fake.user_agent()
    )

def generate_transaction_data(client_id, transaction_type):
    transaction_date = random_date(start_date, end_date)
    currency = random.choice(['USD', 'RUB'])
    amount = round(random.uniform(100, 10000), 2) if currency == 'USD' else round(random.uniform(9000, 900000), 2)
    return Row(
        client_id=client_id,
        transaction_id=random.randint(1000, 10000),
        transaction_date=transaction_date,
        transaction_type=transaction_type,
        account_number=fake.iban(),
        currency=currency,
        amount=amount
    )

def generate_payment_data(client_id, transaction_id):
    payment_date = random_date(start_date, end_date)
    currency = random.choice(['USD', 'RUB'])
    amount = round(random.uniform(100, 10000), 2) if currency == 'USD' else round(random.uniform(9000, 900000), 2)
    return Row(
        client_id=client_id,
        payment_id=random.randint(1000, 10000),
        payment_date=payment_date,
        currency=currency,
        amount=amount,
        payment_method=random.choice(['credit_card', 'debit_card', 'bank_transfer', 'e_wallet']),
        transaction_id=transaction_id  # Связь с транзакцией
    )

# Установите максимальное количество генераций данных
max_generations = 3
current_generation = 0

while current_generation < max_generations:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    login_records = []
    activity_records = []
    payment_records = []
    transaction_records = []

    for _ in range(max_records_per_file):
        client_id = random.randint(1, 10000)

        if random.random() < 0.3:
            login_data = generate_login_data(client_id)
            login_records.append(login_data)

        if random.random() < 0.3:
            activity_data = generate_activity_data(client_id)
            activity_records.append(activity_data)

        # Генерация транзакций
        transaction_type = random.choice(['deposit', 'withdrawal', 'transfer', 'payment'])
        transaction_data = generate_transaction_data(client_id, transaction_type)
        transaction_records.append(transaction_data)

        # Генерация оплаты, если тип транзакции 'payment'
        if transaction_type == 'payment':
            payment_data = generate_payment_data(client_id, transaction_data.transaction_id)
            payment_records.append(payment_data)

    # Преобразование данных в Spark DataFrame
    login_df = spark.createDataFrame(login_records)
    activity_df = spark.createDataFrame(activity_records)
    payment_df = spark.createDataFrame(payment_records)
    transaction_df = spark.createDataFrame(transaction_records)

    # Запись данных в HDFS
    login_df.write.csv(f'hdfs://your/path/to/data/client_logins_{timestamp}', mode='overwrite', header=True)
    activity_df.write.csv(f'hdfs://your/path/to/data/client_activities_{timestamp}', mode='overwrite', header=True)
    payment_df.write.csv(f'hdfs://your/path/to/data/payments_{timestamp}', mode='overwrite', header=True)
    transaction_df.write.csv(f'hdfs://your/path/to/data/bank_transactions_{timestamp}', mode='overwrite', header=True)

    # Увеличиваем счётчик генераций
    current_generation += 1
    print(f"Генерация {current_generation} завершена.")


print("Достигнуто максимальное количество генераций. Остановка программы.")

# Останавливаем Spark сессию
spark.stop()
