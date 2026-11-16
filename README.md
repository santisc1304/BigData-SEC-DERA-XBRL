# Operación XBRL Shield — Analítica Distribuida de Solvencia Corporativa (SEC DERA)

Este repositorio contiene la solución técnica, código fuente y documentación del **Caso de Estudio Big Data 2026-II** de la **Universidad Externado de Colombia**.

- **Entidad Cliente:** U.S. Securities and Exchange Commission (SEC) — Division of Economic and Risk Analysis (DERA).
- **Equipo Consultor:**
  - **Sebastian David Ramos Cuervo** (Componente A: Ingesta HDFS, Replicación 2 y Auditoría de 30 Bloques).
  - **Santiago Sandoval Cortes** (Componente B: SparkSession en YARN, EDA Distribuido, BroadcastHashJoin, Pivot y AQE).
  - **Miguel David Moreno Lozano** (Componente C: Spark UI, Stages 13/15, Caché RAM, Visualización `fmt_co()` y Spark MLlib).

---

## 🌐 Demo Desplegada en Vivo

Puedes explorar y jugar la presentación interactiva 2D directamente en la web:
- **GitHub Pages:** [https://santisc1304.github.io/BigData-SEC-DERA-XBRL/](https://santisc1304.github.io/BigData-SEC-DERA-XBRL/)
- También puedes importar este repositorio en **[Vercel](https://vercel.com)** o **[Cloudflare Pages](https://pages.cloudflare.com)** con 1 solo clic (sin configuración de build, el archivo raíz es `index.html`).

---

## 💻 Ejecución Local 100 % Autónoma (Sin Servidores ni Conexión)

El archivo de la presentación está completamente autocontenido con todas sus imágenes incrustadas en Base64:
- Archivo para entrega local: `presentacion_interactiva_sec.html` (o `index.html`).
- **Instrucciones:** Basta con hacer doble clic sobre el archivo o abrirlo en cualquier navegador (Chrome, Edge, Firefox, Safari) en cualquier computador sin necesidad de internet, carpetas adicionales ni dependencias.

---

## 📂 Estructura del Repositorio

- `presentacion_interactiva_sec.html` / `index.html`: Presentación interactiva 2D temática de la Casa Blanca.
- `context/`:
  - `CasoBD_SEC.ipynb`: Notebook Jupyter con todo el código PySpark ejecutado sobre el clúster YARN.
  - `CasoBD_Ramos-Sandoval-Moreno_Informe.docx`: Informe ejecutivo calibrado a 1 página (490 palabras).
  - `CasoBD_Ramos-Sandoval-Moreno_Bitacora.docx`: Bitácora técnica con tiempos de sesión, `df -h` y comandos.
  - `CasoBD_Ramos-Sandoval-Moreno_Anexos.docx`: Anexos completos con las 14 capturas del clúster (C1 a C11b), incidente I1, recomendación Cloud y referencias APA 7.
- `presentacion_assets/`: Capturas originales extraídas del clúster.
- `graphs/`: Gráfico ejecutivo de solvencia y liquidez por sector SIC con formato numérico colombiano (`fmt_co()`).
