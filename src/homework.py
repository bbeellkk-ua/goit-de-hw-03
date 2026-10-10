"""PySpark analysis for the GOIT data engineering homework."""

from argparse import ArgumentParser
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


USERS_SCHEMA = StructType(
    [
        StructField("user_id", IntegerType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("age", IntegerType(), nullable=True),
        StructField("email", StringType(), nullable=True),
    ]
)

PURCHASES_SCHEMA = StructType(
    [
        StructField("purchase_id", IntegerType(), nullable=True),
        StructField("user_id", IntegerType(), nullable=True),
        StructField("product_id", IntegerType(), nullable=True),
        StructField("date", DateType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
    ]
)

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", IntegerType(), nullable=True),
        StructField("product_name", StringType(), nullable=True),
        StructField("category", StringType(), nullable=True),
        StructField("price", DecimalType(10, 2), nullable=True),
    ]
)


def read_clean_csv(
    spark: SparkSession, path: Path, schema: StructType
) -> DataFrame:
    """Read a header-based CSV with a fixed schema and remove incomplete rows."""
    return (
        spark.read.option("header", True)
        .option("dateFormat", "yyyy-MM-dd")
        .schema(schema)
        .csv(str(path))
        .dropna(how="any")
    )


def calculate_results(
    users: DataFrame, purchases: DataFrame, products: DataFrame
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Calculate category totals, totals for ages 18–25, and their top shares."""
    purchases_with_products = purchases.join(products, "product_id", "inner").withColumn(
        "purchase_amount", F.col("price") * F.col("quantity")
    )

    category_totals = (
        purchases_with_products.groupBy("category")
        .agg(F.sum("purchase_amount").alias("total_amount"))
        .orderBy("category")
    )

    young_purchases = purchases_with_products.join(
        users.filter(F.col("age").between(18, 25)).select("user_id"),
        "user_id",
        "inner",
    )

    young_category_totals = (
        young_purchases.groupBy("category")
        .agg(F.sum("purchase_amount").alias("total_amount_18_25"))
        .orderBy("category")
    )

    total_young_spending = young_category_totals.agg(
        F.sum("total_amount_18_25").alias("all_categories_total")
    )

    category_shares = (
        young_category_totals.crossJoin(total_young_spending)
        .withColumn(
            "spending_percentage",
            F.round(
                F.col("total_amount_18_25")
                / F.col("all_categories_total")
                * F.lit(100),
                2,
            ),
        )
        .select("category", "total_amount_18_25", "spending_percentage")
        .orderBy(F.desc("spending_percentage"), F.asc("category"))
    )

    top_three_categories = category_shares.limit(3)
    return category_totals, young_category_totals, top_three_categories


def parse_args() -> Path:
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Directory containing users.csv, purchases.csv, and products.csv",
    )
    return parser.parse_args().data_dir


def main() -> None:
    data_dir = parse_args()
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("goit-de-hw-03")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        users = read_clean_csv(spark, data_dir / "users.csv", USERS_SCHEMA)
        purchases = read_clean_csv(
            spark, data_dir / "purchases.csv", PURCHASES_SCHEMA
        )
        products = read_clean_csv(spark, data_dir / "products.csv", PRODUCTS_SCHEMA)

        category_totals, young_category_totals, top_three_categories = (
            calculate_results(users, purchases, products)
        )

        print("\n1. Загальна сума покупок за категоріями:")
        category_totals.show(truncate=False)

        print("2. Сума покупок за категоріями для користувачів віком 18–25:")
        young_category_totals.show(truncate=False)

        print("3. Три категорії з найбільшою часткою витрат користувачів 18–25:")
        top_three_categories.show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()