# Databricks notebook source
from pyspark.sql.functions import *


# COMMAND ----------

# Importar base COMPRAS desde la tabla Delta en la capa Bronze
df_compras = spark.sql(
                        '''
                            SELECT venta_id, factura, fecha_orden, fecha_entrega, fecha_envio, estado, cliente_code, nombres, apellidos, departamento, metodo_pago
                            FROM workspace.salesstore.bronze_compras
                        '''
                       )

# COMMAND ----------

# Limpieza de datos: asignar tipos de datos y estandarizar (quitar espacios, convertir a mayusculas, etc)
df_compras = (
                df_compras.withColumn("venta_id", col("venta_id").cast("integer"))
                          .withColumn("fecha_orden", col("fecha_orden").cast("date"))
                          .withColumn("fecha_entrega", try_to_date(col("fecha_entrega"), "dd/MM/yyyy"))
                          .withColumn("fecha_envio", try_to_date(col("fecha_envio"), "dd-MM-yy"))
                          .withColumn("estado", col("estado").cast("integer"))
                          .withColumn("factura", upper(trim(col("factura"))))
                          .withColumn("nombres", initcap(trim(col("nombres"))))
                          .withColumn("apellidos", initcap(trim(col("apellidos"))))
                          .withColumn("departamento", trim(col("departamento")))
                          .withColumn("metodo_pago", trim(col("metodo_pago")))
             )

# Proceso de transformación de datos: Modificación de columnas y creación de columnas calculadas
df_compras = (
                df_compras.withColumn("estado", when(col("estado") == 1, lit("Creado"))
                                                .when(col("estado") == 2, lit("En Curso"))
                                                .when(col("estado") == 3, lit("Programado"))
                                                .when(col("estado") == 4, lit("Cancelado"))
                                                .when(col("estado") == 5, lit("Entregado"))
                                                .otherwise(lit("No Definido")))
                          .withColumn("cliente_id", trim(split(col("cliente_code"), "-").getItem(0)).cast("integer"))
                          .withColumn("num_documento", trim(split(col("cliente_code"), "-").getItem(1)).cast("string"))
                          .withColumn("num_documento", when(length("num_documento") >= 8, col("num_documento"))
                                                .otherwise(lpad(col("num_documento"), 8, "0")))
                          .withColumn("nombre_cliente", concat(col("nombres"), lit(" "), col("apellidos")))
                          .withColumn("dias_envio", when(col("estado") == lit("Entregado"),
                                                         datediff(col("fecha_envio"), col("fecha_orden")))
                                                .otherwise(lit(None)))
                          .withColumn("periodo", date_format(col("fecha_orden"), "yyyyMM"))
            )

# Selección de datos útiles y creación de columna de trazabilidad fecha_carga
df_compras = (
                df_compras.select("periodo", "venta_id", "factura", "fecha_orden", "fecha_entrega", "fecha_envio", "estado", "cliente_id", "num_documento", "nombre_cliente", "departamento", "metodo_pago", "dias_envio")
                .withColumn("fecha_carga", current_timestamp())
             )

# COMMAND ----------

# Importar base DETALLES desde la tabla Delta en la capa Bronze
df_detalles = spark.sql(
                        '''
                            SELECT detalle_id, factura, categoria, subcategoria, producto, unidades, precio_unitario, nombre_archivo
                            FROM workspace.salesstore.bronze_detalles
                        '''
                       )

# COMMAND ----------

# Limpieza de datos: asignar tipos de datos y estandarizar (quitar espacios, convertir a mayusculas, etc)
df_detalles = (
                df_detalles.withColumn("detalle_id", col("detalle_id").cast("integer"))
                          .withColumn("factura", upper(trim(col("factura"))))
                          .withColumn("categoria", trim(col("categoria")))
                          .withColumn("subcategoria", trim(col("subcategoria")))
                          .withColumn("producto", trim(col("producto")))
                          .withColumn("unidades", col("unidades").cast("integer"))
                          .withColumn("precio_unitario", col("precio_unitario").cast("double"))
            )

# Proceso de transformación de datos: Modificación de columnas y creación de columnas calculadas
df_detalles = (
                df_detalles.withColumn("subtotal", col("unidades") * col("precio_unitario"))
                          .withColumn("tienda", trim(split(col("nombre_archivo"), "\\.").getItem(0)).cast("string"))
             )

# Selección de datos útiles y creación de columna de trazabilidad fecha_carga
df_detalles = (
                df_detalles.select("detalle_id", "factura", "tienda", "categoria", "subcategoria", "producto", "unidades", "subtotal")
                .withColumn("fecha_carga", current_timestamp())
             )

# COMMAND ----------

# Carga de datos de la base COMPRAS a la tabla Delta en la capa Silver
(
    df_compras.write
              .format("delta")
              .mode("overwrite")
              #.option("overwriteSchema", "true")
              #.partitionBy("periodo")
              .saveAsTable("workspace.salesstore.silver_compras")
)

# Carga de datos de la base DETALLES a la tabla Delta en la capa Silver
(
    df_detalles.write
                .format("delta")
                .mode("overwrite")
                #.option("overwriteSchema", "true")
                #.partitionBy("periodo")
                .saveAsTable("workspace.salesstore.silver_detalles")
)