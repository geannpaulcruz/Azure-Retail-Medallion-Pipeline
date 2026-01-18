# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

# Importar base COMPRAS desde la tabla Delta en la capa Silver
df_compras = spark.table("workspace.salesstore.silver_compras")

# Importar base DETALLES de la tabla Delta en la capa Silver
df_detalles = spark.table("workspace.salesstore.silver_detalles")

# COMMAND ----------

# Combinar las bases COMPRAS y DETALLES
df_fact_compras = (
                    df_compras.join(df_detalles, "factura", "inner")
                                    .select("Periodo","factura", "venta_id", "fecha_orden", "fecha_envio", "estado", "metodo_pago", "dias_envio", "cliente_id", "num_documento", "nombre_cliente", "departamento", "detalle_id", "tienda", "categoria", "subcategoria", "producto", "unidades", "subtotal")
                        .withColumn("fecha_carga", current_timestamp())
                    )

# COMMAND ----------

# Carga de datos de la base consolidada a la tabla Delta en la capa Gold
(
    df_fact_compras.write
                .format("delta")
                .mode("overwrite")
                .saveAsTable("workspace.salesstore.gold_fact_compras")
)