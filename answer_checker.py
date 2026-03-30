import builtins
import math
import random
import re
from datetime import datetime, date
import os

import pyspark
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import date_format, next_day
from pyspark.sql.functions import hour, to_timestamp
from pyspark.sql.functions import lag
from pyspark.sql.functions import max
from pyspark.sql.functions import (
    translate,
    col,
    lpad,
    format_number,
    regexp_extract,
    concat_ws,
)
from pyspark.sql.window import Window

DEBUG = False

# ===== НАСТРОЙКА ПУТЕЙ =====
BASE_PATH = "/content/extracted_data/"
TABLES_PATH = BASE_PATH
FILESTORE_PATH = "/content/filestore/"

# Создаем директории
os.makedirs(BASE_PATH, exist_ok=True)
os.makedirs(FILESTORE_PATH, exist_ok=True)


congratulations_messages = [
    "Превосходная демонстрация навыков!",
    "Вы демонстрируете выдающиеся достижения!",
    "Точное и полное выполнение поставленной задачи!",
    "Результаты работы выходят за рамки поставленных требований!",
    "Вы демонстрируете глубокое понимание и мастерство!",
    "Предоставленная информация точна и актуальна!",
    "Вы эффективно применили свои знания для решения задачи!",
    "Полное и безошибочное выполнение!",
    "Результаты работы вызывают восхищение!",
    "Вы обладаете необходимыми навыками и способностями!",
    "Исключительные достижения, демонстрирующие высокий уровень знаний и умений!",
    "Вы демонстрируете высокий уровень мастерства в данной области!",
    "Великолепное исполнение!",
    "Вы достигли высокого уровня профессионализма в своей сфере!",
    "Результаты работы производят сильное положительное впечатление!",
    "Вы демонстрируете выдающиеся способности!",
    "Поздравляем с успешным завершением задачи!",
    "Результаты работы демонстрируют высокий уровень компетентности!",
    "Вы обладаете выдающимися способностями и демонстрируете их в работе!",
    "Вы с легкостью управляете и анализируете информацию!",
    "Предоставлено исчерпывающее и верное решение!",
    "Вы - один из лучших в своей области!",
    "Результаты работы демонстрируют высокий уровень качества!",
    "Вы обладаете исключительными способностями!",
    "Вы эффективно работаете с информацией!",
]

try_again_messages = [
    "Пока не совсем, но скоро получится!",
    "Неверный ответ, но не сдавайтесь!",
    "Удачи в следующий раз!",
    "Близки, но не совсем!",
    "Это не совсем верно, но не сдавайтесь!",
    "Почти получилось, продолжайте!",
    "Так близко, но так далеко!",
    "Продолжайте, вы становитесь ближе!",
    "Хорошая попытка, давайте попробуем еще раз!",
    "Не совсем верный ответ, но не беспокойтесь!",
    "Почти идеально, попробуйте еще раз!",
    "Не тот ответ, который мы ищем, но продолжайте!",
    "Вы на правильном пути, продолжайте!",
    "Хорошая попытка, давайте попробуем еще раз!",
    "Отличная попытка, но не совсем верно!",
    "Продолжайте, вы почти там!",
    "Не сдавайтесь, у вас получится!",
    "Достойная попытка, но давайте попробуем еще раз!",
    "Это не совсем верно, но не расстраивайтесь!",
    "Почти, осталось совсем немного!",
    "Не совсем, но вы на правильном пути!",
    "Вы делаете успехи, продолжайте в том же духе!",
    "Почти получилось, попробуйте еще раз!",
    "Хорошая попытка, давайте попробуем еще раз!",
    "Не совсем решение, но мы дойдем до него вместе!",
]


class TestCases:
    def create_df(*args):
        assert isinstance(
            args[0], pyspark.sql.dataframe.DataFrame
        ), "Похоже, что df не является DataFrame от PySpark."
        assert args[0].columns == [
            "date",
            "transaction_id",
            "store_id",
            "currency",
        ], "Столбцы отличаются от тех, что были запрошены в упражнении."
        assert args[0].count() == 3, "Вы добавили ровно одну пользовательскую строку данных?"

    def simple_schema(*args):
        simple_schema = args[0]
        assert isinstance(
            simple_schema, pyspark.sql.types.StructType
        ), "Похоже, что схема не является действительной StructType."
        assert (
            len(simple_schema) == 3
        ), f"Схема должна иметь 3 поля, но имеет {len(simple_schema)}"
        for field, name in zip(
            simple_schema, ["PurchaseDate", "HasPaid", "HasReceivedGood"]
        ):
            assert field.name == name, (
                f"Обнаружена ошибка в наименовании или порядке столбцов при проверке столбца {field.name}."
            )
        for field, type_ in zip(
            simple_schema, ["DateType()", "BooleanType()", "BooleanType()"]
        ):
            assert (
                str(field.dataType) == type_
            ), f"Обнаружена ошибка в типе данных столбца {field.name}."
        for field, nullable in zip(simple_schema, [None, False, True]):
            if field.name == "PurchaseDate":
                continue
            assert (
                field.nullable == nullable
            ), f"Обнаружена проблема с возможностью принимать null-значения в столбце {field.name}."

    def simple_schema_df(*args):
        simple_schema_df = args[0]
        assert isinstance(
            simple_schema_df, pyspark.sql.DataFrame
        ), "Предоставленное значение не является DataFrame от PySpark"
        assert (
            simple_schema_df.count() == 4
        ), "Количество строк не соответствует данным"
        
        # Исправленная проверка схемы
        schema = simple_schema_df.schema
        
        # Проверяем поле HasPaid
        has_paid_field = schema["HasPaid"]
        assert isinstance(has_paid_field.dataType, pyspark.sql.types.BooleanType), "HasPaid должен быть BooleanType"
        assert has_paid_field.nullable == False, "HasPaid должен быть nullable=False"
        
        # Проверяем поле HasReceivedGood
        has_received_field = schema["HasReceivedGood"]
        assert isinstance(has_received_field.dataType, pyspark.sql.types.BooleanType), "HasReceivedGood должен быть BooleanType"
        assert has_received_field.nullable == True, "HasReceivedGood должен быть nullable=True"


    def complex_schema(*args):
        complex_schema = args[0]
        assert isinstance(
            complex_schema, pyspark.sql.types.StructType
        ), "Похоже, что схема не является действительной StructType."
        assert (
            complex_schema[0].name == "PurchaseDate"
        ), "Имя первого столбца отличается от шаблонной."
        assert (
            complex_schema[1].name == "HasPaid"
        ), "Имя второго столбца отличается от шаблонной."
        assert (
            complex_schema[2].name == "HasReceivedGood"
        ), "Имя третьего столбца отличается от шаблонной."
        assert (
            complex_schema[3].name == "TrackingInfo"
        ), "Имя 4-го столбца отличается от шаблонной."
        assert isinstance(
            complex_schema[3].dataType, pyspark.sql.types.ArrayType
        ), "Проверьте тип поля TrackingInfo."
        assert complex_schema[
            3
        ].nullable, (
            "Вы уверены, что nullability для TrackingInfo установлено правильно?"
        )
        assert (
            '"type":"string"' in complex_schema[3].json()
        ), "Тип для TrackingProvider, похоже, не верен."
        assert (
            '"type":"long"' in complex_schema[3].json()
        ), "Тип для TrackingNumber, похоже, не верен."
        assert (
            complex_schema[3].json()
            == '{"metadata":{},"name":"TrackingInfo","nullable":true,"type":{"containsNull":true,"elementType":{'
            '"fields":[{"metadata":{},"name":"TrackingNumber","nullable":false,"type":"long"},{"metadata":{},'
            '"name":"TrackingProvider","nullable":false,"type":"string"}],"type":"struct"},"type":"array"}}'
        ), (
            "Проверьте содержимое TrackingInfo. Проверьте имена столбцов. Для nullability предполагается, что если предоставлен TrackingInfo, "
            "то и TrackingNumber, и TrackingProvider должны иметь значение."
        )

    def changing_column_type(*args):
        modified_schema_df = args[0]
        assert isinstance(
            modified_schema_df, pyspark.sql.DataFrame
        ), "Предоставленное значение не является DataFrame от Spark"
        assert (
            len(modified_schema_df.schema) == 3
        ), "Предоставленный DataFrame не имеет 3 столбцов."
        names = ["PurchaseDate", "HasPaid", "HasReceivedGood"]
        types_ = [
            pyspark.sql.types.StringType,
            pyspark.sql.types.ByteType,
            pyspark.sql.types.BooleanType,
        ]
        for nx, entry in enumerate(modified_schema_df.schema):
            assert (
                entry.name == names[nx]
            ), f"Столбец {nx + 1} не имеет правильного имени."
            assert isinstance(
                entry.dataType, types_[nx]
            ), f'Столбец "{entry.name}" не имеет правильного типа данных'

    def count_transactions_partitions(*args):
        if len(args) != 2:
            raise AssertionError("Вы предоставили неверное количество аргументов.")
        transactions = args[0]
        partition_ct = args[1]
        assert isinstance(
            transactions, pyspark.sql.DataFrame
        ), "Предоставленное значение не является DataFrame от Spark"
        assert (
            partition_ct == transactions.rdd.getNumPartitions()
        ), "`partition_ct` не является числом разделов."

    def repartition_transactions(*args):
        transactions_8p = args[0]
        assert isinstance(
            transactions_8p, pyspark.sql.DataFrame
        ), "Похоже, что `transactions_8p` не является действительным DataFrame."
        assert (
            transactions_8p.rdd.getNumPartitions() == 8
        ), "DataFrame не имеет 8 разделов."

    def create_equal_partition_sizes(*args):
        transactions_eq_partition_sizes = args[0]
        assert isinstance(
            transactions_eq_partition_sizes, pyspark.sql.DataFrame
        ), "Похоже, что переданная переменная не является действительным DataFrame."
        assert (
            transactions_eq_partition_sizes.groupBy(
                pyspark.sql.functions.spark_partition_id()
            )
            .count()
            .toPandas()["count"]
            .std()
            < 10
        ), "Размеры разделов не схожи."

    def caching(*args):
        transactions = args[0]
        assert isinstance(
            transactions, pyspark.sql.DataFrame
        ), "Похоже, что `transactions` не является действительным DataFrame."
        tips = {
            pyspark.StorageLevel.MEMORY_ONLY.__repr__(): "Выбранный вами механизм кэширования недостаточно устойчив для "
            "данного случая использования.",
            pyspark.StorageLevel.MEMORY_AND_DISK.__repr__(): "Доступ к `transactions` будет слишком медленным, когда он "
            "будет выгружен на диск.",
            pyspark.StorageLevel.MEMORY_AND_DISK_2.__repr__(): "Доступ к `transactions` будет слишком медленным, когда он "
            "будет выгружен на диск.",
            pyspark.StorageLevel.DISK_ONLY.__repr__(): "Доступ к `transactions` будет слишком медленным.",
            pyspark.StorageLevel.DISK_ONLY_2.__repr__(): "Доступ к `transactions` будет слишком медленным.",
            pyspark.StorageLevel.OFF_HEAP.__repr__(): "Доступ к `transactions` будет слишком медленным.",
        }

        try:
            raise AssertionError(tips[transactions.storageLevel.__repr__()])
        except KeyError:
            pass

    def reading_parquet_files(*args):
        stores_from_nested = args[0]
        assert isinstance(
            stores_from_nested, pyspark.sql.DataFrame
        ), "Похоже, что `stores_from_nested` не является действительным DataFrame."
        row_count = stores_from_nested.count()
        assert row_count in [2981, 2982], (
            f"DataFrame имеет неверный размер ({row_count}). Вы правильно прочитали все разделы?"
        )

    def reading_csv_files(*args):
        ethans_nightmare_data = args[0]
        assert isinstance(
            ethans_nightmare_data, pyspark.sql.DataFrame
        ), "Похоже, что `ethans_nightmare_data` не является действительным DataFrame."
        
        # Проверка столбцов
        expected_columns = ["sku", "store_id", "year", "month", "way", "distance", "distance_unit", "bundle_size"]
        assert ethans_nightmare_data.columns == expected_columns, "Имена столбцов или их количество некорректны."
        
        # Проверка схемы (надёжный способ)
        schema = ethans_nightmare_data.schema
        expected_types = {
            "sku": pyspark.sql.types.StringType,
            "store_id": pyspark.sql.types.DoubleType,
            "year": pyspark.sql.types.DoubleType,
            "month": pyspark.sql.types.DoubleType,
            "way": pyspark.sql.types.StringType,
            "distance": pyspark.sql.types.DoubleType,
            "distance_unit": pyspark.sql.types.StringType,
            "bundle_size": pyspark.sql.types.DoubleType,
        }
        for col_name, expected_type in expected_types.items():
            actual_type = schema[col_name].dataType
            assert isinstance(actual_type, expected_type), (
                f"Столбец {col_name} должен быть {expected_type.__name__}, но получен {type(actual_type).__name__}"
            )
        
        # Проверка null значений
        assert all(
            [row["way"] is None for row in ethans_nightmare_data.head(4)]
        ), "Значения null не были корректно проанализированы."

    def writing_parquet_files(*args):
        eu_stores_path = f"{FILESTORE_PATH}eu_stores.parquet"
        try:
            dbutils.fs.ls(eu_stores_path)
        except:
            raise AssertionError(
                "Код подготовки к упражнению не был выполнен."
            )

        def is_uncompressed_parquet(path):
            if path.endswith(".parquet") and not path.endswith(".c000.parquet"):
                return False
            return True

        assert all(
            [
                is_uncompressed_parquet(file.path)
                for file in dbutils.fs.ls(f"{eu_stores_path}/country=DE/")
            ]
        ), "Файлы parquet не были сохранены в несжатом формате."

        df_len = spark.read.parquet(eu_stores_path).count()

        if df_len == 1070:
            raise AssertionError(
                f"Данные не были добавлены в `{eu_stores_path}`."
            )
        elif df_len > 1330:
            raise AssertionError(
                f"Некоторые данные были добавлены более одного раза в `{eu_stores_path}`"
            )
        elif df_len == 260:
            raise AssertionError("Оригинальные данные были перезаписаны.")
        elif df_len == 1330:
            pass
        else:
            raise AssertionError(
                f"Общее количество строк в `{eu_stores_path}` некорректно."
            )

        assert all(
            [
                "country=" in file.path
                for file in dbutils.fs.ls(eu_stores_path)
                if file.path.endswith("/")
            ]
        ), "Похоже, что данные не разделены по столбцу страны."

    def writing_csv_files(*args):
        csv_path = f"{FILESTORE_PATH}ethans_nightmare_data.csv/"
        try:
            dbutils.fs.ls(csv_path)
        except:
            raise AssertionError(
                f"В директории `{csv_path}` нет файла."
            )

        csv_files = [
            file.path
            for file in dbutils.fs.ls(csv_path)
            if file.path.endswith(".csv")
        ]
        assert (
            len(csv_files) == 1
        ), f'В директории "{csv_path}" больше одного CSV-файла'

        head = dbutils.fs.head(csv_files[0], 1000)
        assert "|" in head, 'Убедитесь, что в качестве разделителя используется символ "|".'
        assert "<NULL>" in head, 'Убедитесь, что пустые значения выражены как "<NULL>"'

        assert (
            len(
                re.findall(
                    r'"[^"]+"\|"[^"]+"\|"[^"]+"\|"[^"]+"\|"[^"]+"\|"[^"]+"\|"[^"]+"\|"[^"]+"\n',
                    head,
                )
            )
            > 0
        ), 'Убедитесь, что каждое значение заключено в символы ".'

        assert "sku" not in head, "Не должно быть заголовков столбцов."

    def sql_in_pyspark(*args):
        stores_from_sql = args[0]
        assert isinstance(
            stores_from_sql, pyspark.sql.DataFrame
        ), "Похоже, что `stores_from_sql` не является действительным DataFrame."
        assert (
            stores_from_sql.count() == 11774
        ), "SQL-запрос не был корректно применен."
        assert any(
            [x.name == "stores_view" for x in spark.catalog.listTables()]
        ), "Представление с именем `stores_view` не было создано."

    def removing_data(*args):
        store_routes = args[0]
        assert isinstance(
            store_routes, pyspark.sql.DataFrame
        ), "Похоже, что `store_routes` не является действительным DataFrame."
        assert store_routes.columns == [
            "store_id",
            "year",
            "month",
            "way",
        ], "Столбцы не соответствуют спецификации."
        assert (
            store_routes.filter(col("store_id").isNull()).count() == 0
        ), "Столбец `store_id` все еще содержит значения null."
        assert (
            store_routes.filter(col("year").isNull() & col("month").isNull()).count()
            == 0
        ), (
            "Есть строки, в которых и столбец `year`, и столбец `month` "
            "содержат значение null."
        )
        assert (
            store_routes.groupby(["store_id", "year", "way"])
            .count()
            .select("count")
            .select(max("count").alias("c"))
            .take(1)[0]
            .c
            == 1
        ), "Есть более одной записи на магазин в год по пути."

    def modifying_data(*args):
        df_report = args[0]
        assert isinstance(
            df_report, pyspark.sql.DataFrame
        ), "Похоже, что `df_report` не является действительным DataFrame."
        routes = args[1]
        cols = [
            "sku",
            "store_id",
            "year",
            "quarter",
            "vehicle",
            "distance",
            "distance_normalized",
            "distance_unit",
            "investigate",
            "sample",
        ]
        for col_ in cols:
            assert (
                col_ in df_report.columns
            ), f"Этан просил столбец {col_}, но его нет в `df_report`."
        for col_ in df_report.columns:
            assert (
                col_ in cols
            ), f"Вы указали столбец {col_}, но это не столбец, который просил Этан."
        assert [
            x.quarter
            for x in df_report.select("quarter").distinct().orderBy("quarter").collect()
        ] == [None, 1, 2, 3, 4], (
            "Значения в `quarter` кажутся некорректными."
        )
        assert (
            math.ceil(routes.take(1)[0].month / 12 * 4) == df_report.take(1)[0].quarter
        ), "Значения квартала, похоже, назначены некорректно."
        assert builtins.round(
            routes.take(1)[0].distance / routes.take(1)[0].bundle_size
        ) == builtins.round(df_report.take(1)[0].distance_normalized), (
            "Нормализованное расстояние, похоже, было вычислено некорректно."
        )
        assert df_report.filter(col("investigate") == True).count() == 10149, (
            "Значения в столбце `investigate` кажутся некорректными."
        )
        true_pct = df_report.filter(col("sample") == True).count() / df_report.count()
        assert 0.09 < true_pct < 0.11, (
            "Примерно {:.1%} значений в `sample` являются True, "
            "но это должно быть около 10%.".format(true_pct)
        )

    def analyzing_data(*args):
        if len(args) != 2:
            raise AssertionError("Вы предоставили неправильное количество аргументов.")
        table_for_maya = args[0]
        routes = args[1]
        assert isinstance(
            table_for_maya, pyspark.sql.DataFrame
        ), "Похоже, что `table_for_maya` не является действительным DataFrame."

        for col_ in ["year", "month", "sku", "distance"]:
            assert (
                col_ in table_for_maya.columns
            ), f"Столбец {col_} отсутствует в table_for_maya."
        assert (
            len(table_for_maya.columns) == 4
        ), "В таблице больше столбцов, чем указано."
        first_row = table_for_maya.take(1)[0]
        assert (
            routes.filter(
                " and ".join(
                    [f"{col_} == {first_row[col_]}" for col_ in first_row.__fields__]
                )
            )
            .take(1)[0]
            .store_id
            == 1215.0
        ), "Данные не были отфильтрованы по правильному store_id."
        assert (
            routes.filter(
                " and ".join(
                    [f"{col_} == {first_row[col_]}" for col_ in first_row.__fields__]
                )
            )
            .take(1)[0]
            .way
            == "truck"
        ), "Данные не были отфильтрованы по правильному способу транспортировки."
        assert (
            table_for_maya.dropDuplicates().count() == table_for_maya.count()
        ), "В table_for_maya содержатся дубликаты."
        assert (
            180 < table_for_maya.count() < 220
        ), f"Таблица должна содержать около 200 строк, но содержит {table_for_maya.count()}."
        w = Window.orderBy("year")
        diff = col("year") - lag("year", 1).over(w).cast("long")
        assert (
            table_for_maya.withColumn("diff", diff)
            .select(max(col("diff")).alias("max_diff"))
            .collect()[0]
            .max_diff
            >= 0.0
        ), "Таблица не в правильном порядке."

    def dates_and_times(*args):
        df_merger = args[0]
        assert isinstance(
            df_merger, pyspark.sql.DataFrame
        ), "Похоже, что `df_merger` не является действительным DataFrame."
        
        # Проверка столбцов
        for col_ in ["date", "store", "sku", "qty", "report_run_date"]:
            assert col_ in df_merger.columns, f"Столбец {col_} отсутствует в df_merger."
        assert len(df_merger.columns) == 5, "В таблице больше столбцов, чем указано."
        
        first_row = df_merger.take(1)[0]
        
        # Проверка типа date (должен быть timestamp/datetime)
        assert isinstance(
            first_row["date"], datetime
        ), "Столбец `date` не имеет правильного типа данных."
        
        # Проверка типа report_run_date (должен быть date)
        assert isinstance(
            first_row["report_run_date"], date
        ), "Столбец `report_run_date` не имеет правильного типа данных."
        
        # Проверка что report_run_date - четверг
        assert (
            first_row["report_run_date"].weekday() == 3
        ), "Дата в `report_run_date` не является четвергом."
        
        # Проверка что столбец date имеет тип TimestampType
        assert isinstance(
            df_merger.schema["date"].dataType, pyspark.sql.types.TimestampType
        ), "Столбец `date` должен быть типа TimestampType"
        
        # Проверка что столбец report_run_date имеет тип DateType
        assert isinstance(
            df_merger.schema["report_run_date"].dataType, pyspark.sql.types.DateType
        ), "Столбец `report_run_date` должен быть типа DateType"



    def working_with_strings(*args):
        ingestion_df = args[0]
        assert isinstance(
            ingestion_df, pyspark.sql.DataFrame
        ), "Похоже, что `ingestion_df` не является действительным DataFrame."
        for col_ in ["date", "store", "customer_id", "qty", "flagship_store"]:
            assert (
                col_ in ingestion_df.columns
            ), f"Столбец {col_} отсутствует в ingestion_df."
        first_row = ingestion_df.take(1)[0]
        assert (
            first_row["store"].upper() == first_row["store"]
        ), "`store` должен содержать только заглавные буквы."
        assert len(first_row["store"]) == 4, "`store` должен содержать только 4 буквы."

        def test_match_pattern(s):
            pattern = r"^[0-9a-f]{8}-0000-0000-0000-000000000000$"
            return bool(re.match(pattern, s))

        assert test_match_pattern(
            first_row["customer_id"]
        ), "Записи в `customer_id` кажутся неправильным форматом"
        assert isinstance(
            ingestion_df.schema["qty"].dataType, pyspark.sql.types.LongType
        ), "Столбец `qty` не имеет правильного типа."
        assert isinstance(
            ingestion_df.schema["flagship_store"].dataType,
            pyspark.sql.types.BooleanType,
        ), "Столбец `flagship_store` не имеет правильного типа."

    def working_with_arrays(*args):
        focus_skus_and_stores = args[0]
        assert isinstance(
            focus_skus_and_stores, pyspark.sql.DataFrame
        ), "Похоже, что `focus_skus_and_stores` не является действительным DataFrame."
        for col_ in [
            "sku",
            "overlapping",
            "in_flagship_stores",
            "n_stores",
            "store_id",
        ]:
            assert (
                col_ in focus_skus_and_stores.columns
            ), f"Столбец {col_} отсутствует в focus_skus_and_stores."
        assert len(focus_skus_and_stores.columns) == 5, (
            "В focus_skus_and_stores должно быть 5 столбцов."
        )
        assert (
            focus_skus_and_stores.select("sku")
            .groupby("sku")
            .count()
            .select(max("count").alias("max_skus"))
            .take(1)[0]["max_skus"]
            == 3
        ), "В таблице должно быть не более 3 строк с одинаковым sku."
        store_ids = [
            row["store_id"]
            for row in focus_skus_and_stores.filter(col("sku") == "0259162507203")
            .select("store_id")
            .take(3)
        ]
        assert (
            9 in store_ids and 14 in store_ids
        ), "Похоже, что для каждого sku не были выбраны наименьшие store ids"
        assert (
            9 in store_ids and 14 in store_ids and 24 in store_ids
        ), "Похоже, что store ids не были дедуплицированы перед выбором наименьших"
        assert (
            focus_skus_and_stores.filter(col("sku") == "0004685280559")
            .select("in_flagship_stores")
            .take(1)[0]["in_flagship_stores"]
            == True
        ), "Список флагманских магазинов не был правильно учтен."
        assert (
            focus_skus_and_stores.filter(col("sku") == "0100651703937")
            .select("in_flagship_stores")
            .take(1)[0]["in_flagship_stores"]
            == False
        ), "Список флагманских магазинов не был правильно учтен."
        assert (
            focus_skus_and_stores.filter(col("sku") == "0278515309400")
            .select("n_stores")
            .take(1)[0]["n_stores"]
            == 508
        ), "Столбец n_stores не имеет правильных значений"

    def grouping_and_aggregating_part1(*args):
        distance_way_overview = args[0]
        assert isinstance(
            distance_way_overview, pyspark.sql.DataFrame
        ), "Похоже, что `distance_way_overview` не является действительным DataFrame."
        for col_ in ["distance_unit", "air", "boat", "rail", "truck"]:
            assert (
                col_ in distance_way_overview.columns
            ), f"Столбец {col_} должен быть в distance_way_overview."
        assert set(
            distance_way_overview.select(collect_list("distance_unit")).first()[0]
        ) == {
            "km",
            "mi",
            "nmi",
        }, "Столбец distance_unit должен содержать значения km, nmi и mi."

    def grouping_and_aggregating_part2(*args):
        transport_differences = args[0]
        assert isinstance(
            transport_differences, pyspark.sql.DataFrame
        ), "Похоже, что `transport_differences` не является действительным DataFrame."
        for col_ in [
            "store_id",
            "way",
            "minimum_km",
            "average_km",
            "median_km",
            "maximum_km",
            "n_samples",
        ]:
            assert (
                col_ in transport_differences.columns
            ), f"Столбец {col_} должен быть в transport_differences."
        assert transport_differences.columns == [
            "store_id",
            "way",
            "minimum_km",
            "average_km",
            "median_km",
            "maximum_km",
            "n_samples",
        ], "В таблице больше столбцов, чем указал Ethan."
        first_row = transport_differences.take(1)[0]
        assert (
            first_row["store_id"] == 2352.0
        ), 'Первая строка должна быть store_id 2352 с путем "air".'
        col_vals = {
            "minimum_km": 81670,
            "average_km": 89748,
            "median_km": 90000,
            "maximum_km": 92708,
        }
        for col_name, col_val in col_vals.items():
            assert (
                round(first_row[col_name], 0) == col_val
            ), f"Вычисление столбца {col_name} кажется неверным."

    def joining(*args):
        transactions_details = args[0]
        assert isinstance(
            transactions_details, pyspark.sql.DataFrame
        ), "Похоже, что `transactions_details` не является действительным DataFrame."

        cols = [
            "store_id",
            "year",
            "date",
            "transaction_id",
            "customer_id",
            "sku",
            "qty",
            "unit_price",
            "currency",
            "total_km_distance_in_year",
        ]

        for col_ in cols:
            assert (
                col_ in transactions_details.columns
            ), f"Столбец {col_} должен быть в transactions_details."

        for col_ in transactions_details.columns:
            assert (
                col_ in cols
            ), f"Столбец {col_} появляется в transactions_details, но Ethan его не указывал."

        stores = spark.read.parquet(f"{TABLES_PATH}stores.parquet")
        assert (
            stores.filter("revenue_category != 1")
            .select("store_id")
            .join(transactions_details, "store_id")
            .count()
            == 0
        ), "transactions_details должен включать только магазины в revenue_category 1"

        assert (
            round(
                transactions_details.filter(
                    (col("store_id") == 10302) & (col("year") == 2004)
                )
                .select("total_km_distance_in_year")
                .take(1)[0]["total_km_distance_in_year"],
                0,
            )
            == 92797
        ), "Похоже, есть ошибка в расчете total_km_distance_per_year."

        assert (
            transactions_details.count() == 16976
        ), f"Таблица должна содержать 16976 строк, но содержит {transactions_details.count()}."

    def udfs_step3(*args):
        assert len(args) == 3, "Вы должны передать 3 аргумента функции udfs_step3"
        calculate_emissions_udf = args[0]
        emissions = args[1]
        current_emissions_by_way = args[2]

        assert (
            "returnType" in calculate_emissions_udf.__dict__
        ), "calculate_emissions должен быть UDF"

        for way, val in emissions.items():
            df = spark.createDataFrame(
                [(way, 1.0, "km")], ["way", "distance", "distance_unit"]
            )
            assert (
                df.select(
                    calculate_emissions_udf(
                        col("way"), col("distance"), col("distance_unit")
                    )
                ).first()[0]
                == val
            ), f"Ваш UDF, похоже, не работает правильно для {way}"

        df = spark.createDataFrame(
            [(None, 1.0, "km"), ("truck", 1.0, "km")],
            ["way", "distance", "distance_unit"],
        )
        try:
            assert (
                df.select(
                    calculate_emissions_udf(
                        col("way"), col("distance"), col("distance_unit")
                    )
                ).first()[0]
                is None
            ), "Ваш UDF не может обрабатывать значения null в столбце `way`"
        except Exception as e:
            raise AssertionError(
                "Ваш UDF не может обрабатывать значения null в столбце `way`"
            )

        df = spark.createDataFrame(
            [("truck", None, "km"), ("truck", 1.0, "km")],
            ["way", "distance", "distance_unit"],
        )
        try:
            assert (
                df.select(
                    calculate_emissions_udf(
                        col("way"), col("distance"), col("distance_unit")
                    )
                ).first()[0]
                is None
            ), "Ваш UDF не может обрабатывать значения null в столбце `distance`"
        except Exception as e:
            raise AssertionError(
                "Ваш UDF не может обрабатывать значения null в столбце `distance`"
            )

        df = spark.createDataFrame(
            [("truck", 1.0, None), ("truck", 1.0, "km")],
            ["way", "distance", "distance_unit"],
        )
        try:
            assert (
                df.select(
                    calculate_emissions_udf(
                        col("way"), col("distance"), col("distance_unit")
                    )
                ).first()[0]
                is None
            ), "Ваш UDF не может обрабатывать значения null в столбце `distance_unit`"
        except Exception as e:
            raise AssertionError(
                "Ваш UDF не может обрабатывать значения null в столбце `distance_unit`"
            )

        rows = (
            spark.createDataFrame(
                [("boat", 1.0, "nmi"), ("rail", 1.0, "mi")],
                ["way", "distance", "distance_unit"],
            )
            .select(
                calculate_emissions_udf(
                    col("way"), col("distance"), col("distance_unit")
                ).alias("values")
            )
            .take(2)
        )
        assert (
            round(rows[0].values, 2) == 12.96
        ), "Ваш UDF не может обрабатывать единицу `nmi`."
        assert (
            round(rows[1].values, 2) == 38.62
        ), "Ваш UDF не может обрабатывать единицу `mi`."

        assert (
            "emissions" in current_emissions_by_way.columns
        ), "Столбец `emissions` не является частью DataFrame current_emissions_by_way"

    def udfs_step4(*args):
        current_emissions = args[0]
        assert isinstance(
            current_emissions, pyspark.sql.DataFrame
        ), "Похоже, что `current_emissions` не является действительным DataFrame."

        for col_ in ["sku", "store_id", "emissions"]:
            assert (
                col_ in current_emissions.columns
            ), f"Столбец {col_} должен быть в current_emissions."
        assert (
            len(current_emissions.columns) == 3
        ), "current_emissions должен иметь только 3 столбца"
        assert (
            round(
                current_emissions.filter(
                    (current_emissions["sku"] == "9964871126928")
                    & (current_emissions["store_id"] == 8890)
                )
                .take(1)[0]
                .emissions,
                0,
            )
            == 9174813
        ), "Столбец `emissions` кажется не был правильно рассчитан."

    def udfs_step5(*args):
        transactions_with_emissions = args[0]
        assert isinstance(
            transactions_with_emissions, pyspark.sql.DataFrame
        ), "Похоже, что `transactions_with_emissions` не является действительным DataFrame."
        assert transactions_with_emissions.columns[:2] == [
            "sku",
            "store_id",
        ], "Первый столбец должен быть `sku`, а второй - `store_id`."
        assert (
            "emissions" in transactions_with_emissions.columns
        ), "В таблице должен быть столбец `emissions`."
        assert (
            transactions_with_emissions.count() == 298
        ), "В таблице должно быть 298 строк."

    def udfs_step6(*args):
        emissions_per_store = args[0]
        assert isinstance(
            emissions_per_store, pyspark.sql.DataFrame
        ), "Похоже, что `emissions_per_store` не является действительным DataFrame."
        assert (
            len(emissions_per_store.columns) == 2
        ), "emissions_per_store должен иметь ровно 2 столбца."
        assert (
            emissions_per_store.count() == 57
        ), "emissions_per_store должен иметь 57 строк."
        row = emissions_per_store.filter("store_id == 1447").take(1)[0]
        assert (
            round(row[row.__fields__[1]], 0) == 2062898
        ), "Функция агрегации, похоже, не была выбрана правильно."

    def udfs_step7(*args):
        assert len(args) == 2, "Вы должны передать 2 аргумента функции udfs_step7"

        top_stores = args[0]
        bottom_stores = args[1]

        assert isinstance(
            top_stores, pyspark.sql.DataFrame
        ), "Похоже, что `top_stores` не является действительным DataFrame."
        assert isinstance(
            bottom_stores, pyspark.sql.DataFrame
        ), "Похоже, что `bottom_stores` не является действительным DataFrame."

        assert top_stores.count() == 3, "В top_stores должно быть только 3 записи"
        assert (
            bottom_stores.count() == 3
        ), "В bottom_stores должно быть только 3 записи"
        assert {row["store_id"] for row in top_stores.select("store_id").take(3)} == {
            8890,
            8615,
            7950,
        }, "Топовые магазины должны быть 8890, 8615, 7950"
        assert {
            row["store_id"] for row in bottom_stores.select("store_id").take(3)
        } == {6078, 9140, 9493}, "Нижние магазины должны быть 6078, 9140, 9493"


def print_recursive(dir_, add="   "):
    """Функция для рекурсивной печати дерева файлов parquet"""
    for file in dbutils.fs.ls(dir_):
        nicely_formatted_path = file.path.replace(str(dir_), "")
        if file.path.endswith(".parquet"):
            print(f"{add}└─ {nicely_formatted_path}")
        elif file.path.endswith("/"):
            print(f"{add}└─ {nicely_formatted_path}")
            print_recursive(file.path, add=f"{add}   ")


class TestPreparer:
    def reading_parquet_files():
        print("Подготовка упражнения...", end="")
        stores = spark.read.parquet(f"{TABLES_PATH}stores.parquet")
        stores_main_path = f"{FILESTORE_PATH}stores_main"
        stores[stores["size_m2"] < 1000].write.mode("overwrite").parquet(stores_main_path)
        print(".", end="")
        stores[(stores["size_m2"] >= 1000) & (stores["size_m2"] <= 2000)].repartition(
            4
        ).write.mode("overwrite").parquet(f"{stores_main_path}/stores_sub1")
        print(".", end="")
        stores[(stores["size_m2"] >= 2000) & (stores["size_m2"] <= 3000)].repartition(
            2
        ).write.mode("overwrite").parquet(f"{stores_main_path}/stores_sub2")
        print(".", end="")
        stores[stores["size_m2"] >= 4000].repartition(3).write.mode(
            "overwrite"
        ).parquet(f"{stores_main_path}/stores_sub1/stores_subsub")
        print("готово!")
        print()

        print("ДЕРЕВО ФАЙЛОВ")
        print()
        print(f"{stores_main_path}/")
        print_recursive(stores_main_path)

    def reading_csv_files():
        routes = spark.read.csv(
            f"{TABLES_PATH}routes_preview.csv", header=True, inferSchema=True
        )
        reading_csv_path = f"{FILESTORE_PATH}reading_csv_files"
        routes.coalesce(1).replace("TRUCK", None).replace("km", None).orderBy(
            "way"
        ).write.mode("overwrite").csv(
            reading_csv_path,
            sep="|",
            nullValue="<NULL>",
            quoteAll=True,
        )
        csv_path = [
            file.path
            for file in dbutils.fs.ls(reading_csv_path)
            if file.path.endswith(".csv")
        ][0]
        new_csv_path = f"{FILESTORE_PATH}SUSTNB_FINAL2_TEST1_REV3_ETHAN.CSV"
        dbutils.fs.cp(csv_path, new_csv_path)

        print(f'Содержимое CSV "{new_csv_path}"')
        print()
        print(dbutils.fs.head(new_csv_path, 1000) + "[...]")

    def writing_parquet_files():
        stores = spark.read.parquet(f"{TABLES_PATH}stores.parquet")
        eu_stores = stores[stores["country"].isin(["UK", "FR", "DE", "ES", "PL"])]
        eu_stores_path = f"{FILESTORE_PATH}eu_stores.parquet"
        eu_stores[eu_stores["revenue_category"].isin([3, 4])].write.mode(
            "overwrite"
        ).partitionBy("country").parquet(
            eu_stores_path, compression="uncompressed"
        )
        print("ДЕРЕВО ФАЙЛОВ")
        print()
        print(f"{eu_stores_path}/")
        print_recursive(eu_stores_path)

        high_revenue_eu_stores = eu_stores[eu_stores["revenue_category"].isin([1, 2])]
        return high_revenue_eu_stores

    def writing_csv_files():
        routes = spark.read.csv(
            f"{TABLES_PATH}routes_preview.csv", header=True, inferSchema=True
        )
        ethans_nightmare_data = (
            routes.repartition(3)
            .replace("TRUCK", None)
            .replace("km", None)
            .orderBy("way")
        )
        return ethans_nightmare_data

    def dates_and_times():
        transactions = spark.read.parquet(f"{TABLES_PATH}transactions_0.parquet")
        transactions_merger_df = (
            transactions.filter('currency == "USD"')
            .sample(0.02, seed=31)
            .withColumn(
                "PURCHASE TIME", date_format("date", "M/d/yyyy K.mm a").cast("string")
            )
            .withColumn(
                "DAY REPORTED",
                (date_format(next_day("date", "Mon"), "M/d/yyyy K.mm a")).cast(
                    "string"
                ),
            )
            .withColumn("STORE", (col("store_id") + 9999999).cast("string"))
            .withColumnRenamed("sku", "STOCK KEEPING UNIT")
            .withColumnRenamed("qty", "QUANTITY")
            .orderBy("date", "store", "sku")
            .select(
                "PURCHASE TIME",
                "DAY REPORTED",
                "STORE",
                "STOCK KEEPING UNIT",
                "QUANTITY",
            )
        )
        return transactions_merger_df

    def working_with_strings():
        transactions = spark.read.parquet(f"{TABLES_PATH}transactions_0.parquet")
        merger_sales = (
            transactions.filter('currency == "USD"')
            .sample(fraction=0.2, seed=95)
            .select(
                translate(
                    lpad(col("sku").cast("string"), 4, ""), "0123456789", "ABCdefabCDE."
                ).alias("STORE"),
                translate(
                    lpad(col("customer_id"), 8, ""),
                    "0123456789abcdef",
                    "FEDCBA0123456789",
                ).alias("CUSTOMER"),
                format_number(col("qty"), 2).alias("QTY"),
                translate(
                    regexp_extract(col("date").cast("string"), r"([0-9\-]+)", 1),
                    "-",
                    "",
                ).alias("DATE"),
            )
            .select(concat_ws(" ", "STORE", "CUSTOMER", "QTY", "DATE").alias("SALES"))
        )
        return merger_sales

    def working_with_arrays():
        transactions = spark.read.parquet(f"{TABLES_PATH}transactions_0.parquet")
        skus_by_store = (
            transactions.groupby("sku")
            .agg(collect_list("store_id").alias("sold_at_stores"))
            .withColumn("overlapping", col("sku").contains("55"))
        )
        return skus_by_store


def prepare(exercise):
    """Подготовка данных для упражнения"""
    try:
        res = getattr(TestPreparer, exercise)()
    except Exception as err:
        print(
            "🤨 Упражнение не может быть подготовлено. Это не ваша вина, код для проверки, вероятно, содержит ошибку."
        )
        if DEBUG:
            print(err)
            raise err
        return None
    return res


def check_answer(*args, exercise=None):
    """Проверка ответа на упражнение"""
    try:
        getattr(TestCases, exercise)(*args)
    except AssertionError as err:
        print(f"❌ {random.choice(try_again_messages)} {str(err)}")
    except Exception as err:
        print(
            "🤨 Результат не может быть проверен. Это не ваша вина, код для проверки, вероятно, содержит ошибку."
        )
        if DEBUG:
            raise err
    else:
        print(f"✅ {random.choice(congratulations_messages)}")


print("✅ answer_checker загружен и готов к работе!")
print(f"📁 Путь к данным: {TABLES_PATH}")
print(f"📁 Путь FileStore: {FILESTORE_PATH}")
