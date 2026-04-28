from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper, trim

def validate_customers(df: DataFrame):
    """Validar clientes: State deve ser CA."""
    valid = df.filter(upper(trim(col("State"))) == "CA")
    invalid = df.filter(upper(trim(col("State"))) != "CA")
    print(f"Valid customers (State=CA): {valid.count()}")
    print(f"Invalid customers (State!=CA): {invalid.count()}")
    return valid, invalid

def validate_items(df: DataFrame):
    """Validar itens: CustomerID não pode ser nulo."""
    valid = df.filter(col("CustomerID").isNotNull())
    invalid = df.filter(col("CustomerID").isNull())
    print(f"Valid items: {valid.count()}")
    print(f"Invalid items (missing CustomerID): {invalid.count()}")
    return valid, invalid
