
from datetime import datetime, timedelta
import random

from airflow.sdk import DAG, TaskGroup
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator


def check_postgres_connection():
    hook = PostgresHook(postgres_conn_id="postgres_src")

    result = hook.get_first(
        "SELECT current_database(), current_user;"
    )

    print(f"PostgreSQL connection successful: {result}")

def reset_raw_tables():

    hook = PostgresHook(postgres_conn_id="postgres_src")

    hook.run("""
        TRUNCATE TABLE
            raw.order_items,
            raw.orders,
            raw.customers,
            raw.products
        RESTART IDENTITY;
    """)

    print("Raw tables successfully cleared")


def generate_and_load_customers():

    # Генерируем исходные данные
    first_names = [
        "Алексей", "Иван", "Максим", "Дмитрий", "Артём",
        "Анна", "Мария", "Екатерина", "Ольга", "Елена"
    ]

    last_names = [
        "Иванов", "Петров", "Сидоров", "Смирнов",
        "Кузнецов", "Попова", "Волкова", "Соколова"
    ]

    cities = [
        "Москва", "Казань", "Санкт-Петербург",
        "Екатеринбург", "Новосибирск", "Самара"
    ]

    customers = []

    for i in range(1, 101):
        customers.append((
            random.choice(first_names),
            random.choice(last_names),
            f"customer{i}@example.com",
            random.choice(cities)
        ))

    # Подключаемся к PostgreSQL
    hook = PostgresHook(postgres_conn_id="postgres_src")


    # Загружаем покупателей
    hook.insert_rows(
        table="raw.customers",
        rows=customers,
        target_fields=[
            "first_name",
            "last_name",
            "email",
            "city"
        ],
        commit_every=100
    )

    print(f"Загружено покупателей: {len(customers)}")


def generate_and_load_products():

    product_names = [
        "Смартфон", "Ноутбук", "Наушники", "Монитор",
        "Клавиатура", "Футболка", "Джинсы", "Кроссовки",
        "Куртка", "Рюкзак", "Кофемашина", "Пылесос",
        "Микроволновка", "Блендер", "Утюг",
        "Гантели", "Коврик для йоги", "Велотренажер",
        "Часы", "Очки", "Кошелек", "Ремень"
    ]

    categories = [
        "Электроника",
        "Одежда",
        "Бытовая техника",
        "Спорт",
        "Аксессуары"
    ]

    products = []

    for i in range(1, 31):
        products.append((
            f"{random.choice(product_names)} {i}",
            random.choice(categories),
            round(random.uniform(500, 100000), 2)
        ))

    hook = PostgresHook(postgres_conn_id="postgres_src")

    hook.insert_rows(
        table="raw.products",
        rows=products,
        target_fields=[
            "product_name",
            "category",
            "price"
        ],
        commit_every=100
    )

    print(f"Загружено товаров: {len(products)}")

def generate_and_load_orders():

    hook = PostgresHook(postgres_conn_id="postgres_src")

    # Получаем ID существующих покупателей
    customer_ids = hook.get_records(
        "SELECT customer_id FROM raw.customers;"
    )

    customer_ids = [row[0] for row in customer_ids]

    # Генерируем заказы
    orders = []

    now = datetime.now()
    start_date = now - timedelta(days=90)

    statuses = [
        "completed",
        "processing",
        "cancelled"
    ]

    for i in range(300):

        customer_id = random.choice(customer_ids)

        order_date = start_date + timedelta(
            seconds=random.randint(
                0,
                int((now - start_date).total_seconds())
            )
        )

        status = random.choice(statuses)

        orders.append((
            customer_id,
            order_date,
            status
        ))

    # Загружаем заказы в PostgreSQL
    hook.insert_rows(
        table="raw.orders",
        rows=orders,
        target_fields=[
            "customer_id",
            "order_date",
            "status"
        ],
        commit_every=100
    )

    print(f"Загружено заказов: {len(orders)}")

def generate_and_load_order_items():

    hook = PostgresHook(postgres_conn_id="postgres_src")

    # Получаем существующие заказы
    orders = hook.get_records(
        "SELECT order_id FROM raw.orders;"
    )

    order_ids = [row[0] for row in orders]

    # Получаем товары вместе с их ценами
    products = hook.get_records(
        "SELECT product_id, price FROM raw.products;"
    )

    order_items = []

    # Генерируем позиции для каждого заказа
    for order_id in order_ids:

        # Случайное количество разных товаров в заказе
        number_of_products = random.randint(1, 5)

        selected_products = random.sample(
            products,
            number_of_products
        )

        for product_id, price in selected_products:

            quantity = random.randint(1, 5)

            order_items.append((
                order_id,
                product_id,
                quantity,
                price
            ))

    # Загружаем позиции заказов
    hook.insert_rows(
        table="raw.order_items",
        rows=order_items,
        target_fields=[
            "order_id",
            "product_id",
            "quantity",
            "unit_price"
        ],
        commit_every=100
    )

    print(f"Загружено позиций заказов: {len(order_items)}")


with DAG(
    dag_id="retail_pipeline",
    description="ETL pipeline for retail analytics",
    start_date=datetime(2026, 11, 13),
    schedule=None,
    catchup=False,
    tags=["retail", "ETL"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    task_check = PythonOperator(
        task_id="check_postgres_connection",
        python_callable=check_postgres_connection
    )

    with TaskGroup(group_id="generate_raw_data") as generate_raw_data:
        task_customers = PythonOperator(
            task_id="generate_customers",
            python_callable=generate_and_load_customers
        )

        task_products = PythonOperator(
            task_id="generate_products",
            python_callable=generate_and_load_products
        )

        task_orders = PythonOperator(
            task_id="generate_orders",
            python_callable=generate_and_load_orders
        )

        task_order_items = PythonOperator(
            task_id="generate_and_load_order_items",
            python_callable=generate_and_load_order_items
        )

        task_customers >> task_orders

        [task_orders, task_products] >> task_order_items

    task_reset = PythonOperator(
        task_id="reset_raw_tables",
        python_callable=reset_raw_tables
    )

    task_dbt_build = BashOperator(
    	task_id="dbt_build",
    	bash_command="mkdir -p /tmp/dbt-logs /tmp/dbt-target && dbt build --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --log-path /tmp/dbt-logs --target-path /tmp/dbt-target",
    )

    finish = EmptyOperator(
        task_id="finish"
    )

    start >> task_check >> task_reset >> generate_raw_data >> task_dbt_build >> finish


