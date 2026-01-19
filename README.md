Pipeline de Ingesta Híbrida y Arquitectura Medallion (Retail)

Descripción
Solución de ingeniería de datos diseñada para la optimización de reportes retail, eliminando procesos manuales de extracción de datos. Este proyecto automatiza el flujo de información desde fuentes heterogéneas hasta un modelo de datos listo para la toma de decisiones estratégica.

Arquitectura del Sistema
![Diagrama de Arquitectura](architecture/arquitectura_proyecto.png)

Tecnologías y Herramientas
Azure Data Factory: Orquestación y extracción de datos desde GitHub (JSON) y FTP (CSV).

Azure Data Lake Storage Gen2: Repositorio central organizado en capas Bronze, Silver y Gold.

Azure Databricks: Procesamiento distribuido utilizando Spark SQL y PySpark para la transformación de datos.

Power BI: Visualización de KPIs críticos y análisis de tendencias de ventas.

Resultados del Proyecto
Orquestación en Data Factory
![Orquestación ADF](images/adf_pipeline_success.png)

Procesamiento en Databricks (Capas Medallion)
![Workflow Databricks](images/azb_workflow_success.png)

Dashboard Ejecutivo Final
![Dashboard Power BI](images/dashboard_sales.png)
