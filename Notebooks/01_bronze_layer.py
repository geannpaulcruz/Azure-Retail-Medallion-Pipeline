# Databricks notebook source
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, ShortType

# COMMAND ----------

file_path_compras = r'abfss://materiales@saproyectoventitas.dfs.core.windows.net/Materiales/Compras/Facturas.json'
folder_path_detalles = r'abfss://materiales@saproyectoventitas.dfs.core.windows.net/Materiales/Detalles/'

# COMMAND ----------

df_compras = (
                spark.read.format("json").load(file_path_compras)
            )

df_compras = df_compras.select(
                                col("VentaID").alias("venta_id"),
                                col("Factura").alias("factura"),
                                col("Fecha_Orden").alias("fecha_orden"),
                                col("Fecha_Entrega").alias("fecha_entrega"),
                                col("Fecha_Envio").alias("fecha_envio"),
                                col("Estado").alias("estado"),
                                col("Cliente_Code").alias("cliente_code"),
                                col("Nombres").alias("nombres"),
                                col("Apellidos").alias("apellidos"),
                                col("Departamento").alias("departamento"),
                                col("Metodo_Pago").alias("metodo_pago"),
                                col("FechaImportacion").alias("fecha_importacion")
                                )
df_compras = df_compras.withColumn("fecha_carga", current_timestamp())

# COMMAND ----------

df_detalles = (
                spark.read.format('CSV')
                            .option("header",True)
                            .option("sep",",")
                            .option("inferSchema","false")
                            .load(folder_path_detalles)
            )

df_detalles = df_detalles.select(
                                col("Detalle_ID").alias("detalle_id"),
                                col("Factura").alias("factura"),
                                col("Categoria").alias("categoria"),
                                col("Subcategoria").alias("subcategoria"),
                                col("Producto").alias("producto"),
                                col("Unidades").alias("unidades"),
                                col("Precio_Unitario").alias("precio_unitario")
                                )
df_detalles = df_detalles.withColumn("nombre_archivo",col("_metadata.file_name")) \
    .withColumn("fecha_carga", current_timestamp())

# COMMAND ----------

# Cargar datos de base COMPRAS a la tabla Delta en la capa Bronze
(
    df_compras.write
                .format("delta")
                .mode("overwrite")
                #.option("overwriteSchema", "true")
                .saveAsTable("workspace.salesstore.bronze_compras")
)

# Cargar datos de base DETALLES a la tabla Delta en la capa Bronze
(
    df_detalles.write
                .format("delta")
                .mode("overwrite")
                #.option("overwriteSchema", "true")
                .saveAsTable("workspace.salesstore.bronze_detalles")
)