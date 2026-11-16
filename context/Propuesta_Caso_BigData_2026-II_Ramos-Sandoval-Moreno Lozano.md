UNIVERSIDAD EXTERNADO DE COLOMBIA

Departamento de Matemáticas — Programa de Ciencia de Datos
Asignatura: Big Data (Período 2026-II)
PROPUESTA DE SELECCIÓN DE CONJUNTO DE DATOS

A la atención de la docente de la asignatura:
A continuación, presentamos nuestra propuesta de conjunto de datos seleccionada para el desarrollo del Caso de Estudio de Big Data,
estructurada conforme a los criterios técnicos y de evaluación de la Sección III de los lineamientos oficiales.

1. Nombres completos de cada integrante y grupo:

Integrante

Nombre Completo

Integrante 1

Sebastian David Ramos Cuervo

Integrante 2

Santiago Sandoval Cortes

Integrante 3

Miguel David Moreno Lozano

Grupo

MABD01

MABD01

MABD02

* Nota: Equipo conformado de manera mixta. Nos comprometemos formalmente a asistir a las tres sesiones de ejecución en la misma franja horaria

autorizada, conforme a lo estipulado en la Sección I del caso.

2. Tema de interés y problema que se pretende abordar para la entidad contratante:

(cid:127) Entidad contratante (cliente hipotético): U.S. Securities and Exchange Commission (SEC) — Division of Economic and Risk Analysis
(DERA).
(cid:127) Tema de interés: Finanzas corporativas masivas y analítica distribuida de solvencia empresarial.
(cid:127) Problema a abordar: Todas las empresas públicas que cotizan en bolsa reportan trimestralmente sus estados financieros bajo el
estándar XBRL. La DERA de la SEC requiere procesar de manera distribuida el histórico continuo de los últimos cuatro trimestres
(2025-Q4, 2026-Q1, 2026-Q2 y 2026-Q3) para construir un sistema analítico que normalice los hechos contables de miles de
corporaciones (enlazando balances generales, estados de resultados y flujos de caja). El objetivo es calcular indicadores sectoriales de
apalancamiento, liquidez y riesgo crediticio por industria (código SIC), así como la evolución interanual del desempeño financiero
corporativo, superando los cuellos de botella de memoria que impiden este análisis en arquitecturas no distribuidas.

3. URL oficial del conjunto de datos:

(cid:127) Portal institucional oficial: https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets
(cid:127) Nota operativa de descarga: En la sección inferior del catálogo oficial DERA se listan los paquetes trimestrales directos en formato
comprimido: 2025q4.zip, 2026q1.zip, 2026q2.zip y 2026q3.zip.

4. URL de respaldo en nube propia del equipo:

(cid:127) Enlace de Google Drive con los datos respaldados:
https://drive.google.com/drive/folders/1ZpjYG7BrwgBiZ_V3adZx9EI-lRi-mtTC?usp=sharing

5. Tamaño descomprimido confirmado:

(cid:127) Estrategia de consolidación: Se integran de forma coherente los tres trimestres del año fiscal 2026 (2026q1, 2026q2, 2026q3) junto
con el trimestre de cierre del año anterior (2025q4), consolidando un período completo de 4 trimestres consecutivos (12 meses de análisis
continuo).
(cid:127) Tamaño comprimido total: 263,8 MB descargados (la compresión en ZIP supera el 90 % debido a la estructura de texto tabulado
repetitivo).
(cid:127) Tamaño total descomprimido confirmado en disco: 2,84 GB (2.910,5 MB), cumpliendo plenamente el rango reglamentario exigido de
2 GB a 5 GB.
    - 2025q4: 644,6 MB descomprimido.
    - 2026q1: 640,5 MB descomprimido.
    - 2026q2: 685,0 MB descomprimido.
    - 2026q3: 940,3 MB descomprimido.

6. Fecha de última actualización del conjunto de datos:

15 de octubre de 2026 (fecha de publicación oficial y corte de radicación del tercer trimestre Q3-2026 por parte de la SEC), cumpliendo
con el requisito de actualización posterior al 1 de julio de 2026.

7. Cobertura temporal de los datos:

Abarca desde el 1 de octubre de 2025 hasta el 30 de septiembre de 2026 (completando cuatro trimestres continuos: Q4-2025,
Q1-2026, Q2-2026 y Q3-2026), cubriendo ampliamente 9 meses de registros del año 2026.

8. Primera línea del archivo o esquema del conjunto de datos:

Los datos presentan un formato relacional tabular normalizado, delimitado por tabuladores (\t):

A. Archivo principal consolidado num.txt (Hechos contables numéricos — ~2,48 GB descomprimido):
(cid:127) Primera línea (Header): adsh   tag   version   coreg   ddate   qtrs   uom   value   footnote
(cid:127) Esquema de variables:
    - adsh: Identificador único de envío EDGAR (Accession Number - String)
    - tag: Etiqueta contable estandarizada US-GAAP / IFRS (e.g., Assets, Revenues, NetIncomeLoss - String)
    - version: Versión de la taxonomía XBRL utilizada (String)
    - coreg: Sub-entidad o co-registrante (String)
    - ddate: Fecha de corte contable reportada YYYYMMDD (Integer / Date)
    - qtrs: Duración temporal en trimestres (0 = Balance instantáneo, 1 = Trimestre, 4 = Anual) (Integer)
    - uom: Unidad de medida financiera (USD, shares, pure) (String)
    - value: Monto numérico del hecho contable (Double)
    - footnote: Identificador de notas al pie explicativas (String)

B. Archivo de dimensiones sub.txt (Metadatos de las compañías — ~7,7 MB descomprimido):
(cid:127) Primera línea (Header): adsh   cik   name   sic   countryba   stprba   cityba   form   period   fy   fp   filed   accepted
(cid:127) Esquema de variables:
    - adsh: Clave foránea para unir relacionalmente con num.txt (String)
    - cik: Identificador central corporativo asignado por la SEC (Integer)
    - name: Razón social oficial de la empresa (String)
    - sic: Código de clasificación industrial estándar (4 dígitos) (Integer)
    - countryba / cityba: Ubicación geográfica de la sede central corporativa (String)
    - form: Tipo de formulario regulatorio (10-K anual, 10-Q trimestral, 20-F) (String)
    - period: Fecha de corte del balance financiero YYYYMMDD (Integer)
    - filed: Fecha de radicación oficial ante la SEC YYYYMMDD (Integer)

Quedamos atentos a sus comentarios y aprobación formal para iniciar la preparación del código en Google Colab sobre una muestra y
registrar la entrada S0 en la bitácora de equipo.

Cordialmente,
Sebastian David Ramos Cuervo  |  Santiago Sandoval Cortes  |  Miguel David Moreno Lozano

