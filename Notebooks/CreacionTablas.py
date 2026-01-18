# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.salesstore;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.salesstore.bronze_compras (
# MAGIC   venta_id STRING,
# MAGIC   factura STRING,
# MAGIC   fecha_orden STRING,
# MAGIC   fecha_entrega STRING,
# MAGIC   fecha_envio STRING,
# MAGIC   estado STRING,
# MAGIC   cliente_code STRING,
# MAGIC   nombres STRING,
# MAGIC   apellidos STRING,
# MAGIC   departamento STRING,
# MAGIC   metodo_pago STRING,
# MAGIC   fecha_importacion STRING,
# MAGIC   fecha_carga TIMESTAMP
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION 'abfss://materiales@saproyectoventitas.dfs.core.windows.net/bronze/compras'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.salesstore.bronze_detalles (
# MAGIC   detalle_id STRING,
# MAGIC   factura STRING,
# MAGIC   categoria STRING,
# MAGIC   subcategoria STRING,
# MAGIC   producto STRING,
# MAGIC   unidades STRING,
# MAGIC   precio_unitario STRING,
# MAGIC   nombre_archivo STRING,
# MAGIC   fecha_importacion STRING,
# MAGIC   fecha_carga TIMESTAMP
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION 'abfss://materiales@saproyectoventitas.dfs.core.windows.net/bronze/detalles'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.salesstore.silver_compras (
# MAGIC   periodo STRING,
# MAGIC   venta_id INT,
# MAGIC   factura STRING,
# MAGIC   fecha_orden DATE,
# MAGIC   fecha_entrega DATE,
# MAGIC   fecha_envio DATE,
# MAGIC   estado STRING,
# MAGIC   cliente_id INT,
# MAGIC   num_documento STRING,
# MAGIC   nombre_cliente STRING,
# MAGIC   departamento STRING,
# MAGIC   metodo_pago STRING,
# MAGIC   dias_envio INT,
# MAGIC   fecha_carga TIMESTAMP
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION 'abfss://materiales@saproyectoventitas.dfs.core.windows.net/silver/compras'
# MAGIC PARTITIONED BY (periodo)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.salesstore.silver_detalles (
# MAGIC   detalle_id INT,
# MAGIC   factura STRING,
# MAGIC   tienda STRING,
# MAGIC   categoria STRING,
# MAGIC   subcategoria STRING,
# MAGIC   producto STRING,
# MAGIC   precio_unitario DOUBLE,
# MAGIC   unidades INT,
# MAGIC   subtotal DOUBLE,
# MAGIC   fecha_carga TIMESTAMP
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION 'abfss://materiales@saproyectoventitas.dfs.core.windows.net/silver/detalles'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.salesstore.gold_fact_compras (
# MAGIC   periodo STRING,
# MAGIC   factura STRING,
# MAGIC   venta_id INT,
# MAGIC   fecha_orden DATE,
# MAGIC   fecha_envio DATE,
# MAGIC   dias_envio INT,
# MAGIC   estado STRING,
# MAGIC   metodo_pago STRING,
# MAGIC   cliente_id INT,
# MAGIC   num_documento STRING,
# MAGIC   nombre_cliente STRING,
# MAGIC   departamento STRING,
# MAGIC   detalle_id INT,
# MAGIC   tienda STRING,
# MAGIC   categoria STRING,
# MAGIC   subcategoria STRING,
# MAGIC   producto STRING,
# MAGIC   unidades INT,
# MAGIC   subtotal DOUBLE,
# MAGIC   fecha_carga TIMESTAMP
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION 'abfss://materiales@saproyectoventitas.dfs.core.windows.net/gold/fact-compras'
# MAGIC PARTITIONED BY (periodo)