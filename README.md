# Proyecto Data Engineer - Ventas con BigQuery

Este proyecto carga datos de ventas desde un CSV a Google BigQuery, realiza consultas para obtener las prendas más caras y vendidas, y guarda los resultados en CSV locales.

## Tecnologías

- Python 3.12  
- pandas, pyarrow, pandas-gbq, db-dtypes  
- Google Cloud BigQuery  

## Configuración

1. Descargar y colocar la clave JSON de la cuenta de servicio (`key.json`) en la carpeta del proyecto.  
2. Setear variable de entorno en PowerShell:

```powershell
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\Users\alane\OneDrive\Desktop\proyecto\key.json"
