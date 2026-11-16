UNIVERSIDAD EXTERNADO DE COLOMBIA
Departamento de Matemáticas — Programa de Ciencia de Datos
Asignatura: Big Data
Período académico 2026-II
CASO DE ESTUDIO – BIG DATA
I. DESCRIPCIÓN DEL CASO
Los equipos de trabajo abordarán un ejercicio de consultoría en el dominio de Big Data. El cliente es
la entidad oficial que publica el conjunto de datos y que, hipotéticamente, contrata al equipo porque
eltamañodelosarchivosnohapermitidosuanálisisconlosrecursosdisponiblesensuorganización.
Elcasoesdeprocesamientoporlotes(batch):elanálisisseejecutaconApacheSparksobreYARN,
conalmacenamientoenHDFS.ApacheKafkaySparkStructuredStreamingnoseimplementanenel
caso;setrabajanenlosTalleres10y11.Laextensiónastreamingsolopuedemencionarse,deforma
opcional,enlarecomendacióncloudvs.on-premisedelosanexos.
Conformación de los equipos
Se conformarán doce (12) equipos: nueve (9) de tres integrantes y tres (3) de dos integrantes. Los
integrantes de un equipo pertenecen al mismo grupo (MABD01 o MABD02), dado que las sesiones
de ejecución se desarrollan en el horario de cada grupo. Por regla general, los integrantes de un
equipo pertenecen al mismo grupo (MABD01 o MABD02). Se admiten equipos con estudiantes de
ambos grupos únicamente con autorización previa de la docente, solicitada en el correo de
conformacióndelequipo.Laautorizaciónseotorgacontrescondiciones:todoslosintegrantesasisten
a las tres sesiones de ejecución (9, 11 y 13 de noviembre) en una misma franja, la de MABD01 o la
deMABD02,indicadaenlasolicitudysincambiosposteriores;elclústerdereferenciaperteneceaun
integrantedelgrupocuyafranjaseeligió;ylaasistenciadecadaintegranteseregistraúnicamenteen
lafranjaautorizada.
La conformación se reporta por correo electrónico a la docente, con copia a todos los integrantes,
antes del viernes 23 de octubre de 2026 a las 5:00 p. m. El asunto sigue el formato [BigData2026-
II]Conformacióndeequipo-<Apellido1>,<Apellido2>,<Apellido3>.
Lostresprimerosequiposquesolicitenporcorreoelectrónicoconformarsecondosintegrantes,dentro
del mismo plazo, quedarán autorizados. Si al cierre del plazo no se completan las tres solicitudes, la
docente asignará los equipos de dos integrantes entre los estudiantes que no se hayan organizado
enequiposdetres.
Clúster de referencia
ElHDFSdecadaclústerresideenlasmáquinasvirtualesdelequipofísicodelasalaDeepLearning.
Por esta razón, cada equipo de trabajo elige el clúster de uno de sus integrantes (clúster de
referencia) y trabaja en ese mismo equipo físico durante las tres sesiones de ejecución. El número
delequipofísicoyelnombredelintegrantepropietarioseregistranenlabitácora.
Si el clúster de referencia queda inutilizable, el equipo documenta el hecho como incidente operativo
(sección IX), carga de nuevo el conjunto de datos desde su respaldo en nube en el clúster de otro
integrante y registra el cambio en la bitácora. En ese caso, el application_id de referencia pasa a ser
eldelaprimeraejecuciónexitosaenelnuevoclúster.
BigData2026-II·Casodeestudio·Página1

II. SELECCIÓN DEL CONJUNTO DE DATOS
Cada equipo seleccionará un tema de interés articulado con el alcance del programa de Ciencia de
Datos (finanzas, ambiente, salud, logística, educación, seguridad, movilidad, energía, entre otros) y
unconjuntodedatosúnico,provenientedefuentesoficialesnacionalesointernacionales,quecumpla
lossiguientescriterios:
• Tamaño: entre 2 GB y 5 GB descomprimido. Si se trabaja con archivos comprimidos (CSV.gz,
JSON.gz, entre otros), el tamaño descomprimido debe cumplir este rango. Se admite el
muestreo, o la unión de varios archivos coherentes (por ejemplo, varios meses) para alcanzar
eltamañorequerido.
• Actualización: la fecha de última actualización del conjunto de datos es posterior al 1 de julio
de2026.
• Cobertura temporal: los registros incluyen al menos un mes de 2026. Este criterio evita
conjuntoscuyaactualizacióncorrespondesoloametadatosynoalcontenidosustantivo.
• Unicidad: cada conjunto de datos se aprueba a un solo equipo en el período 2026-II. Si dos
equipos proponen el mismo conjunto, se aprueba al equipo cuyo correo de propuesta llegó
primero. Otra versión del mismo conjunto (otro período, un subconjunto o una réplica) cuenta
comoelmismoconjunto.
Fuentes de datos permitidas
Se aceptan como fuentes oficiales los portales nacionales (datos.gov.co, DANE, Minciencias, Banco
de la República, entre otros), los portales internacionales (data.gov, Eurostat, Organización Mundial
de la Salud, FAO, Banco Mundial, entre otros) y los repositorios oficiales de organizaciones
reconocidas. Se acepta HuggingFace Datasets como fuente alternativa siempre que el conjunto de
datosoriginalseaoficialyreal,contrazabilidadhacialaentidadpublicadoraprimaria.
Recomendaciones operativas
Respaldoennube.Lasrestriccionesdeciberseguridadinstitucionalesimpidenconectardispositivos
dealmacenamientoexternosenlosequiposdelasalaDeepLearning.Porello,semantieneunacopia
delconjuntodedatosenunanubepropiadelequipo(GoogleDrive,OneDrivesieltamañolopermite,
HuggingFace, entre otras), accesible mediante una URL que permita descargarla directamente al
nodomaestrosilaURLoriginalquedabloqueadaporlaspolíticasdeseguridaddelainstitución.
Descarga directa al nodo maestro. El conjunto de datos se descarga directamente en el nodo
maestro con wget o curl, o mediante la API de la fuente cuando exista, en lugar de descargarlo en el
equipofísicoytransferirlodespués.UnavezcargadoenHDFS,lacopialocalseelimina.
Verificación de espacio en disco. Varios equipos de la sala tienen poco espacio libre, y los discos
delasmáquinasvirtualescrecenconcadaarchivoquerecibensinreducirseautomáticamentecuando
el archivo se elimina. Antes de la sesión 1 se verifica el espacio libre del equipo físico (df -h en una
terminal del anfitrión) y se registra en la bitácora. Como referencia, conviene disponer al menos del
tripledeltamañodelconjuntodedatos:unacopialocaltemporalydosréplicasenHDFS.
Carga sin copia local. Para reducir el espacio requerido, el archivo puede enviarse a HDFS sin
guardarloantesenelnodomaestro,porquehdfsdfs-putacepta-comoorigenyleedesdelaentrada
estándar:
hdfsdfs-mkdir-p/caso
wget-qO-"<URL>"|hdfsdfs-put-/caso/<archivo>.csv
BigData2026-II·Casodeestudio·Página2

#Archivocomprimidocongzip:
wget-qO-"<URL>"|gunzip-c|hdfsdfs-put-/caso/<archivo>.csv
Si la descarga se interrumpe, el archivo queda incompleto en HDFS: se elimina con hdfsdfs-rm y se
repiteelcomando.Eltamañofinalseverificaconhdfsdfs-du-s-h<ruta>yelfactordereplicacióncon
hdfsdfs-stat%r<ruta>,quedebedevolver2.
Preparación del código en Google Colab. Antes de la sesión 1, el código se desarrolla en Google
Colabsobreunamuestradelconjuntodedatos(porejemplo,unmesdelosvariosquelocomponen),
con la SparkSession en modo local, como en la guía de primeros pasos con PySpark en Colab y la
actividadasincrónicadel19deoctubre.Estetrabajoeslaactividadpreviadelasesión1yseregistra
enlabitácora(entradaS0).
III. VALIDACIÓN PREVIA DEL CONJUNTO DE DATOS
Como paso obligatorio previo al inicio del trabajo, cada equipo envía una propuesta del conjunto de
datosalcorreoelectrónicodeladocenteparasuaprobación.Estavalidacióngarantizaquelafuente,
el tamaño y la actualidad cumplan los criterios antes de invertir tiempo en la configuración y la
ejecuciónsobreelclúster.
Asuntodelcorreo:[BigData2026-II]Propuestacasodeestudio-<Apellido1>,<Apellido2>,<Apellido3>
Contenidomínimodelcorreo:
1. Nombrescompletosdecadaintegranteygrupo(MABD01oMABD02).
2. Temadeinterésyproblemaquesepretendeabordarparalaentidadcontratante.
3. URLoficialdelconjuntodedatos.
4. URLderespaldoennubepropiadelequipo.
5. Tamaño descomprimido confirmado, con captura de pantalla del comando ls -lh o du -sh
ejecutadosobreelarchivo.
6. Fechadeúltimaactualizacióndelconjuntodedatos.
7. Coberturatemporaldelosdatos(rangodefechasqueabarcanlosregistros).
8. Primera línea del archivo o esquema del conjunto de datos, para confirmar el formato tabular
procesable.
Plazos de envío y revisión
El plazo recomendado para enviar la propuesta es el viernes 30 de octubre de 2026 a las 11:00 a.
m. La docente responde la aprobación o el rechazo dentro de las 16 horas hábiles siguientes a la
recepcióndelapropuesta.
El miércoles 4 de noviembre a las 11:00 a. m. es el último plazo con respuesta garantizada antes
de la sesión 1. Se reciben propuestas posteriores, pero la pérdida de tiempo que ocasionen durante
lassesionesdeejecuciónlaasumeelequipo.
IV. CONJUNTOS DE DATOS NO PERMITIDOS
Los siguientes conjuntos de datos no pueden utilizarse, dado que se emplearon en períodos
académicos anteriores o en las sesiones de clase de la asignatura. La restricción busca asegurar la
originalidaddelanálisisyevitarlareutilizacióndetrabajosdeperíodosanteriores.
BigData2026-II·Casodeestudio·Página3

Restricciones por tipo de conjunto (cualquier ciudad, fuente o período)
• Registros de viajes de taxi y de plataformas de transporte (por ejemplo, NYC TLC Trip Record
Data en todas sus variantes, Chicago Taxi Trips y Chicago Transportation Network Providers –
Trips).
• Solicitudesdeservicio311(porejemplo,Chicago311ServiceRequestsySF311Cases).
Restricciones por conjunto específico (cualquier período, subconjunto o réplica)
Conjuntodedatos Fuenteopublicador
GlobalPowerPlantDatabase WorldResourcesInstitute
Deslizamientos(GlobalLandslideCatalog) NASA
AccidentalidadvialdeBarranquilla datos.gov.co
E-commercecustomers JoséPortilla(conjuntosintético)
Ecommercebehaviordatafrommulti-categorystore Kaggle
Sistemacrediticiocolombiano datos.gov.co
ConjuntosderivadosdeSpotify Variasfuentes
Partidasdeajedrez lichess.org
SECOPII ColombiaCompraEficiente
OpenTenderEU OpenTender
EmergenciasenNuevaYork NYCOpenData
ViolacionesdeestacionamientodeNuevaYork NYCOpenData
CrímenesenNuevaYork NYCOpenData
CitiBikeNYC CitiBike
Airline/flightdata USDepartmentofTransportation
Datosmeteorológicosglobales Variasfuentes
ComisiónparaelEsclarecimientodelaVerdad CEV
JurisdicciónEspecialparalaPazyHRDAG JEP,HRDAG
FoodDataCentral USDA
PricePaidData GobiernodelReinoUnido
DesempleoenColombia DANE,datos.gov.co
FondosdeinversióncolectivaenColombia datos.gov.co
VSPCdataset —
UTIdataset —
AISBroadcastData(AutomaticIdentificationSystem) NOAA,MarineCadastre
MTASubwayHourlyRidership data.ny.gov
Crimes–2001toPresent ChicagoDataPortal
ReddeMonitoreodeCalidaddelAire(g4t8-zkc3)yconjuntos IDEAM,datos.gov.co
derivados
Canalesdedistribucióntransaccional SuperintendenciaFinanciera,
datos.gov.co
BigData2026-II·Casodeestudio·Página4

Conjuntodedatos Fuenteopublicador
Lesionespersonales datos.gov.co
Hurtoapersonas datos.gov.co
Multasdetránsito(SIMIT) datos.gov.co
Las restricciones incluyen cualquier otra versión del mismo conjunto: otro período, un subconjunto o
una réplica publicada en otro repositorio (HuggingFace, Kaggle, entre otros). En caso de duda sobre
laadmisibilidaddeunconjuntonolistado,seconsultapreviamenteconladocente.
V. CRONOGRAMA DEL CASO DE ESTUDIO
Fecha Hito
Lunes19deoctubre Publicacióndeloslineamientos,larúbricaylasplantillasenMicrosoftTeams.
Viernes23deoctubre, Cierredelaconformacióndeequiposydelassolicitudesdeequiposdedos
5:00p.m. integrantes.
Viernes30deoctubre, Plazorecomendadoparaelenvíodelapropuestadelconjuntodedatos.
11:00a.m.
Miércoles4de Últimoplazoconrespuestagarantizadaantesdelasesión1.
noviembre,11:00a.m.
Antesdellunes9de Conjuntodedatosaprobadoycargadoenlanubederespaldo;código
noviembre preliminardesarrolladoenColabsobreunamuestra;espacioendisco
verificado;bitácoraconlaentradaS0.
Lunes9denoviembre IngestaaHDFS,configuracióndelaSparkSessionsobreYARN,primera
(sesión1) ejecuciónexitosa(application_iddereferencia)yexploracióninicial.
Miércoles11de Análisisprincipal,aplicacióndeoptimizacionesdondecorrespondaycapturade
noviembre(sesión2) métricas.
Viernes13de Ajustesfinales,capturasfaltantes,bonificación(siaplica)yverificacióndel
noviembre(sesión3) estadodelclúster.Ladocenteenvíaporcorreoelectrónicoelresultadodel
sorteodefranjasdesustentación.
Sábado14alunes16de Trabajoindependiente:informe,anexos,presentaciónycierredelabitácora.
noviembre
Martes17de Entregatécnica,enunsoloenvíoporMicrosoftTeams.
noviembre,8:00a.m.
Miércoles18de Sustentaciónoral.Cadaequipoasisteúnicamenteensufranjaasignada.
noviembre,7:00a11:00
a.m.
Nota. La sesión del 9 de noviembre no es para buscar el conjunto de datos. A esa fecha, el conjunto
debe estar aprobado, cargado en la nube de respaldo y acompañado del código preliminar de carga
yprocesamiento.Lastressesionessededicanalaejecuciónsobreelclúster,únicoentornoenelque
esposibleobtenerlasevidenciasexigidas.
VI. REQUERIMIENTOS TÉCNICOS DEL ANÁLISIS
Eldesarrolloserealizasobreelclústerdereferencia(unnodomaestroydosnodostrabajadores).El
procesamiento se ejecuta con Apache Spark sobre YARN, con soporte de HDFS. El lenguaje
obligatorioesPySparkyelentornodetrabajoesJupyterNotebooksobreelclúster.
BigData2026-II·Casodeestudio·Página5

Requerimientos obligatorios
1. Almacenamiento en HDFS. Carga del conjunto de datos a HDFS con factor de replicación 2,
verificable en el portal web de HDFS, y documentación de la distribución de bloques entre los
DataNodes.
2. Configuración de la SparkSession sobre YARN. Configuración explícita, con justificación de
cada parámetro que se modifique respecto a su valor por defecto (por ejemplo,
spark.executor.memory, spark.executor.cores, spark.sql.shuffle.partitions o
spark.sql.autoBroadcastJoinThreshold).
3. Análisis exploratorio. Con la API de DataFrames o con Spark SQL: esquema, conteos,
distribuciones,identificacióndevaloresnulosycaracterizacióngeneraldelconjuntodedatos.
4. Análisis principal. Al menos dos transformaciones complejas. Se considera transformación
compleja:unjoin(preferiblementeconunatablapequeña,paraanalizarlaestrategiadedifusión
o broadcast), una agregación agrupada con varias métricas o una cadena de al menos cuatro
transformaciones consecutivas evaluadas perezosamente. Las funciones de ventana
(Window.partitionBy())seadmitencomoopción,peronoseexigen.
5. Plan de ejecución. Uso de .explain() sobre al menos una operación crítica, con interpretación
del plan. Se incluye una nota sobre Adaptive Query Execution (AQE): la presencia de
isFinalPlan=false en el plan inicial y su contraste con el plan final que muestra la pestaña SQL /
DataFramedelaSparkUI.
6. AnálisisdelaSparkUI.Identificacióndelstagedemayorduraciónquenoseaelstagefinalde
salida,justificacióndelcuellodebotellasegúnelvolumendedatosylasoperacionesdeshuffle,
y reporte de las métricas de shuffle (datos leídos, escritos y spill a disco, si aplica). Se
documentan al menos dos stages relevantes, cada uno con su captura, el DAG con su
explicación, sus métricas (duración, número de tareas, shuffle read / write y spill, si aplica) y la
justificacióndesurelevancia.
7. Visualización de resultados. Aplicación del patrón Spark (procesamiento distribuido)
→ .toPandas() (materialización del resultado agregado) → ecosistema Python local (pandas,
NumPy,matplotlibuotrasbibliotecas).Lamaterializacióncon.toPandas()serealizasolodespués
de agregaciones que reduzcan significativamente el tamaño del resultado. Las visualizaciones
siguenloslineamientosdelaasignaturaVisualizaciónIypresentanlascifrasdetablasygráficos
enformatonuméricocolombiano(puntoparamilesycomaparadecimales),porejemploconla
funciónfmt_co()delaSesión33,salvoautorizaciónexpresadeladocente.
8. Cache o persistencia (condicional). Se aplica solo si un DataFrame es reutilizado por dos o
másacciones.Siseaplica,seespecificaelstoragelevel,sejustificasuelección,sepresentala
evidenciadelapestañaStoragedelaSparkUIyseliberalamemoriaconunpersist()cuandoel
DataFramedejadeusarse.Sinoseaplica,seincluyeunalíneaqueexpliqueporquéelanálisis
noreutilizaningúnDataFrame.
9. Anexos con evidencia del clúster. Capturas prescritas con criterio de suficiencia y hoja de
registrodevaloresobservados,segúnlasecciónXylaplantilladeanexos.Porstagerelevante
se entiende aquel que cumple al menos uno de estos criterios: (a) presenta la mayor duración
del job analizado, (b) maneja el mayor volumen de shuffle (escritura o lectura), (c) genera spill
a disco o (d) contiene la operación crítica del análisis (por ejemplo, el join principal o la
agregaciónfinal).
Sobre el application_id de referencia
La captura del portal de YARN con el application_id corresponde a la primera ejecución exitosa de
la sesión 1 (9 de noviembre). Dado que cada reinicio de la SparkSession genera un nuevo
application_id, esta exigencia permite verificar la trazabilidad del trabajo desde su inicio. El
application_idtieneelformatoapplication_<timestamp>_<id>,dondeeltimestampcorrespondealinicio
delResourceManager.Losapplication_iddecadasesiónseregistranademásenlabitácora.
BigData2026-II·Casodeestudio·Página6

VII. POLÍTICA DE APLICACIÓN DE OPTIMIZACIONES
Lastécnicasdeoptimizaciónseaplicandondesonnaturalmenteapropiadassegúnlascaracterísticas
delconjuntodedatosydelanálisis.Nosefuerzasuusocuandonoaportanvalorreal:
• FormatoParquet:solosihaysalidasintermediasofinalesquesereutilicenocompartan.
• Cache o persistencia: solo si un DataFrame es reutilizado por dos o más acciones
(requerimiento8).
• Broadcastjoin:solocuandohayjoinscontablaspequeñas,pordebajodelumbralconfigurado
enspark.sql.autoBroadcastJoinThreshold.
• Método.explain():siemprequeserealicenjoins,paraverificarlaestrategiaelegida,ademásde
laoperacióncríticadelrequerimiento5.
Una optimización aplicada sin necesidad, o sin justificación, no suma en la rúbrica. Una decisión
justificadadenoaplicarlasísevalora.
VIII. BONIFICACIONES EXCLUYENTES
Cada equipo puede optar por una sola de las siguientes profundizaciones, excluyentes entre sí. La
aplicación efectiva y bien documentada de la profundización elegida suma puntos sobre la nota de
equipodelaentregatécnica.
Bonificación Descripción Puntaje
máximo
ConstruccióndeunpipelinedeMachineLearningconVectorAssembler,
Pipelinecon
entrenamientodeunmodelo(regresión,clasificaciónoclustering),cálculo +0,5
SparkMLlib
demétricasdeevaluacióneinterpretaciónargumentadadelosresultados.
Ejecucióndelamismaoperacióndejoincon
SortMergeJoin spark.sql.autoBroadcastJoinThreshold=-1(loquefuerzaSortMergeJoin)y
vs.Broadcast luegoconelvalorpordefecto,concapturadelostiemposdeejecución,las +0,3
HashJoin métricasdeshuffle(datosescritosyleídos)ylosplanesfísicosdeambos
escenarios.
Labonificaciónsesumaalanotadeequipoantesdeaplicarelfactorindividual(secciónXIII),ylanota
delaentregatécnicapuedesuperar5,0.Lanotadefinitivadelaasignaturanosupera5,0.Laevidencia
delabonificacióncorrespondealacapturaC11delosanexos.
IX. POLÍTICA DE INCIDENTES OPERATIVOS
Siduranteunasesiónsepresentanincidentesoperativosdelclúster(caídasdenodos,problemasde
red, fallos de hardware, errores de memoria, contenedores YARN terminados, falta de espacio en
disco, entre otros), el equipo los documenta en la sección de incidentes de los anexos y los registra
enlabitácora.Ladocumentaciónincluyeelcontextodelincidente,losmensajesdeerrorobservados
consuscapturasdepantalla(confechayhoravisibles)ylasaccionestomadaspararesolverlo.
Los incidentes documentados se valoran positivamente como evidencia de trabajo real sobre el
clúster.Laausenciadeincidentesnopenalizaalosequiposcuyaejecuciónhayasidofluida.
X. ENTREGA TÉCNICA
Plazo:martes17denoviembrede2026alas8:00a.m.
BigData2026-II·Casodeestudio·Página7

Plataforma: tarea «Caso de estudio – entrega técnica» en el equipo de Microsoft Teams de la
asignatura.
Envío único. Todos los entregables se suben en un mismo envío. A las 8:00 a. m. la tarea se cierra
y no admite entregas posteriores; no se reciben entregas tardías. Se recomienda subir con
anticipaciónloqueexistayreemplazarloporlaversiónfinalantesdelcierre,dadoqueTeamsadmite
reemplazarlaentregamientraslatareapermanezcaabierta.
Falla de Microsoft Teams. La entrega se intenta primero en Teams. Si la plataforma falla, se toma
una captura de pantalla del error con la fecha y la hora visibles y se envían la captura y todos los
entregables al correo electrónico de la docente. El correo debe llegar antes de las 8:00 a. m. del 17
de noviembre. Si el tamaño de los archivos lo exige, el envío puede dividirse en varios correos,
siemprequetodoslleguenantesdeesahora.
Entregables
1. Informe de consultoría en PDF, con extensión máxima de una (1) página: tamaño carta,
márgenes de 3 cm, letra Times New Roman de 12 puntos e interlineado sencillo. Presenta el
contexto, el objetivo, la metodología resumida, los resultados clave y las conclusiones con
recomendaciones, en estilo de consultoría ejecutiva dirigida a la entidad contratante. Las
referenciasvanenlosanexos.
2. Anexos en PDF, elaborados sobre la plantilla de anexos publicada en Teams (contenido en
estamismasección).
3. Notebook (.ipynb) con el código completo y comentado, la configuración de la SparkSession y
lasceldasdesalidadelaejecuciónenelclúster.
4. Presentación en PDF para la sustentación oral, organizada por los componentes A, B y C
(sección XI), con una portada y como máximo tres diapositivas por componente. En la
sustentación se proyecta únicamente este archivo; no se admiten cambios posteriores a la
entrega.
5. Bitácora de equipo en PDF, elaborada sobre la plantilla de bitácora, con la firma de todos los
integrantes(secciónXII).
Nombres de archivo: CasoBD_<Apellido1>-<Apellido2>-<Apellido3>_<Entregable>, por ejemplo
CasoBD_Gomez-Ruiz-Torres_Informe.pdf.
Contenido de los anexos
Losanexossiguenelordendelaplantilla:
• A1.Capturasobligatorias, consuanálisis.
• A2. Hoja de registro de valores observados, con la fuente exacta de cada valor: pestaña y
campodelportal,celdadelnotebookocomando.
• A3.Comandosdeejecución documentados(descarga,cargaaHDFS,verificación).
• A4.Incidentesoperativos, siloshubo.
• A5.Recomendacióncloudvs.on-premise.
• A6.Declaracióndeusodeinteligenciaartificial (secciónXI).
• A7.Referenciasbibliográficas enformatoAPA,séptimaedición.
BigData2026-II·Casodeestudio·Página8

A1. Capturas obligatorias
N.º Req. Quédebeverseenlacaptura
C1 1 PortalwebdeHDFS(NameNode,puerto9870),pestañaDatanodes:losdosDataNodes
enservicio,consucapacidadyusovisibles.
C2 1 PortalwebdeHDFS,Utilities›Browsethefilesystem:elarchivoodirectoriodel
conjuntodedatos,consutamañoyelvalor2enlacolumnaReplication.
C3 1 Salidaenterminaldehdfsfsck<ruta>-files-blocks-locations:elresumenfinal(Totalsize,
Totalblocks,Averageblockreplication)ylaubicacióndealmenosunbloqueenlos
dosDataNodes.Sinocabeenunaimagen,seadmitenC3ayC3b.
C4 2 PortaldeYARN(ResourceManager,puerto8088):laaplicacióndelaprimeraejecución
exitosadel9denoviembre,consuapplication_id,nombre,usuario,horadeinicioy
estadovisibles.
C5 2 SparkUI,pestañaEnvironment,secciónSparkProperties:losparámetros
configuradosenlaSparkSession.Siserequiere,seadmiteC5bconlapestaña
Executors.
C6 5 Salidade.explain()delaoperacióncríticaenelnotebook:laceldacompleta,conlalínea
AdaptiveSparkPlanisFinalPlan=falsevisible.
C7 5 SparkUI,pestañaSQL/DataFrame:detalledelamismaconsulta,conelplanfinalyla
líneaisFinalPlan=trueenlasecciónDetails.
C8 6 SparkUI,pestañaStages:páginadedetalledelprimerstagerelevante,conDAG
VisualizationylatablaSummaryMetricsforCompletedTasks(Duration,Shuffle
Read/WriteySpill,siaplica).SeadmitenC8ayC8b.
C9 6 IgualqueC8,paraelsegundostagerelevante.SeadmitenC9ayC9b.
C10 8(condicional) Solosiseaplicacacheopersistencia.SparkUI,pestañaStorage:elDataFrame
persistido,conStorageLevel,FractionCached,SizeinMemoryySizeonDisk.
C11 Bonificación Solosiseoptaporunabonificación.SMJvs.BHJ:planfísicoymétricasdeshufflede
(condicional) cadaejecución(seadmitenC11aaC11d).MLlib:salidadelaceldaconlasmétricasde
evaluacióndelmodelo.
Criteriodesuficiencia:
• Todaslascapturaspresentanlafechaylahoradelsistema.
• En las capturas de la Spark UI (C5, C7 a C11) se ve la barra de direcciones del navegador: la
URLcontieneelidentificadorapplication_…,queacreditaquelaejecuciónespropia.
• En las capturas de salidas del notebook (C6 y C11 de MLlib) se ve la celda completa, incluido
elcódigoqueproducelasalida.
• Enlacapturadeterminal(C3)seveelcomandocompleto,conelpromptdelusuariohadoop.
• Cada imagen se relaciona en su subsección de la plantilla de anexos, en el orden indicado,
acompañadadeunanálisisdedosacuatrolíneasqueinterpretaloquemuestra.
• Se exigen únicamente estas capturas. Una captura adicional no otorga puntos; la ausencia de
unaobligatoriadejasinrespaldolosrequisitosquedependendeella.Lascapturasdeincidentes
operativossepresentanensupropiasección.
A2. Hoja de registro
La hoja de registro de la plantilla reúne los valores observados en la ejecución propia que respaldan
el informe y las capturas: tamaño y bloques en HDFS, configuración de la SparkSession, plan de
ejecución, métricas de los dos stages relevantes, tiempos y, si aplica, cache. Cada valor va
acompañado de dónde se leyó. Un valor sin fuente no se considera evidencia. Un valor ausente
BigData2026-II·Casodeestudio·Página9

tambiénesundato:siunamétricanoapareceenelportal,seregistralapalabra«ninguno»yseindica
dóndesebuscó.
A5. Recomendación cloud vs. on-premise
Párrafo de aproximadamente 150 palabras, dirigido a la entidad contratante, que recomienda si el
análisis realizado debería procesarse en un clúster on-premise como el utilizado (Hadoop y Spark) o
enunserviciocloudserverlesscomoBigQuery.Larecomendación:
• seapoyaenalmenosunvalorobservadoenelclústerpropio(porejemplo,eltiempodelanálisis
principal o los recursos configurados) y en al menos un valor observado en la práctica de
BigQuery(porejemplo,losbytesprocesadosporunaconsulta);
• consideraelcosto,laescalabilidad,elcontroloperativoylacurvadeaprendizaje;
• puedemencionar,deformaopcional,laextensióndelanálisisaprocesamientoenstreaming.
XI. COMPONENTES Y DECLARACIÓN DE USO DE INTELIGENCIA
ARTIFICIAL
Componentes del trabajo
El trabajo se divide en cuatro componentes. Los componentes A, B y C, se usan para declarar la
autoría,sortearlaexposiciónenlasustentaciónygraduarelfactorindividual;elcomponenteDesde
responsabilidadcompartida.Cadaintegrantedeclaracomopropioalmenosunodeloscomponentes
A,BoC;enlosequiposdedosintegrantes,unodeellosdeclarados.
Componente Alcance
A Datosyalmacenamiento Selecciónyjustificacióndelconjuntodedatos,ingestaaHDFS,factorde
replicaciónydistribucióndebloques.
B Procesamiento ConfiguracióndelaSparkSession,análisisexploratorio,
transformacionescomplejas,.explain()ynotasobreAQE.
C Desempeñoyresultados AnálisisdelaSparkUI,optimizaciones,visualizaciones,conclusiones,
recomendacióncloudvs.on-premiseybonificación,siaplica.
D Redacción Informedeconsultoríaypresentación.
Declaración de uso de IA
El uso de inteligencia artificial está permitido en el caso de estudio, en concordancia con los
Lineamientos para el Uso de Inteligencia Artificial de la Universidad Externado de Colombia
(Comunicaciónrectoraln.º004del4deoctubrede2024).Ladeclaraciónesobligatoria,seincluyeen
lasecciónA6delosanexosyesinsumodelfactorindividual.Sucontenido:
1. Tabla de uso por componente (A, B, C y D), con el integrante responsable, la categoría de
uso, la herramienta y una descripción breve del uso. La categoría se selecciona del numeral
2.1.2 del lineamiento institucional: Revisado con IA, Redactado con IA, Corregido con IA o
Traducido con IA; se registra «Sin uso de IA» cuando corresponda. La categoría Desarrollado
porIA(numeral2.1.1)noseadmitesincontribuciónintelectualsignificativadelequipo.
2. Contribuciónintelectualsignificativadelequipo,conunaextensiónmáximade150palabras.
IdentificalasdecisionestécnicastomadassinasistenciadeIA:seleccióndelconjuntodedatos,
definición del objetivo del análisis, justificación de las optimizaciones e interpretación de los
resultados.
BigData2026-II·Casodeestudio·Página10

3. Declaración firmada por todos los integrantes, en la que afirman haber revisado el contenido
final y verificado la exactitud de la información generada con apoyo de IA, en cumplimiento del
numeral4.5.3dellineamientoinstitucional.
La evaluación del aprendizaje se basa en la comprensión demostrada en la sustentación oral y en la
consistencia entre el código entregado, los resultados obtenidos, la bitácora y la declaración de uso
deIA.
XII. BITÁCORA DE EQUIPO Y FACTOR INDIVIDUAL
Bitácora de equipo
La bitácora registra la contribución de cada integrante por componente y es la fuente documental del
factorindividual.SediligenciaenlaplantillapublicadaenTeams,desdelapreparaciónprevia(entrada
S0) hasta el trabajo independiente posterior a la sesión 3. Se recomienda mantenerla en un archivo
deOneDriveoGoogleDrivecompartidoporlosintegrantes,cuyohistorialdeversionesmuestraquién
editóycuándo.
Cada entrada registra la fecha, el integrante, el componente, la actividad realizada, la evidencia
verificable (application_id, celda del notebook, captura o comando) y el uso de IA. La bitácora se
entregaenPDF,firmadaportodoslosintegrantes,juntoconlosdemásentregables(AnexoB).
Factor individual
La nota de la entrega técnica es una nota de equipo, ajustada para cada integrante por un factor
individualdetresniveles:
Factor Descriptor
Contribuciónplena:laevidenciadocumentaldelabitácoraylasustentaciónsonconsistentesentre
1,0
síyconladeclaracióndeusodeIA.
Contribuciónparcial,oinconsistenciaentrelodeclaradoylosustentadoenalgunodelos
0,85
componentes.
0,7 Contribuciónmarginal,verificableperomínima.
Procedimiento.Ladocentecontrastalaevidenciadecontribuciónporcomponenteconsignadaenla
bitácora con la declaración de uso de IA y registra un nivel provisional antes de la sustentación oral.
Lasustentaciónconfirmaomodificaesenivel.Larúbricaaplicadasearchivaconfecha.
Autoría no acreditada. Se resuelve antes del factor y produce 0,0 en la entrega técnica. Exige, de
forma concurrente, la ausencia de evidencia documental de contribución en la bitácora y la
imposibilidad de sustentar los componentes declarados como propios. La concurrencia protege al
estudiantequeaportóysedesempeñamalenlaexposiciónoral.
XIII. SUSTENTACIÓN ORAL
Lasustentaciónserealizaelmiércoles18denoviembrede2026,entrelas7:00ylas11:00a.m.,
enformatodeequipoconvaloraciónindividual.Cadaequipoasisteúnicamenteensufranjaasignada.
Lasfranjasseasignanporsorteoysecomunicanporcorreoelectrónicoelviernes13denoviembre.
Al inicio de la franja, la docente asigna por sorteo el componente (A, B o C) que expone cada
integrante, sin conocimiento previo del equipo. Concluida la exposición, cada integrante responde
preguntas individuales sobre el componente que declaró como propio y sobre un componente ajeno.
BigData2026-II·Casodeestudio·Página11

Lacalificaciónseconstruyeúnicamenteconelsegmentoexpuestoyconlasrespuestasindividuales;
lacalidadgeneraldelaexposicióndelequiponointervieneenella.Elprotocolocompletosepresenta
enelAnexoCylarúbricaenelAnexoA.3.
XIV. EVALUACIÓN
Componentedelaasignatura Peso Valoración
Casodeestudio–entregatécnica 25% Notadeequipoajustadaporelfactorindividual
Casodeestudio–sustentaciónoral 10% Individual
Nota de la entrega técnica
Lanotadeequipoeslasumaponderadadelosnivelesalcanzadosenloscuatrocriteriosdelarúbrica
(AnexoA.1):
Criterio Peso
Seleccióndelconjuntodedatos 10%
Calidaddelanálisisyresultados 25%
UsodetecnologíasdeBigData 45%
Informeyanexos 20%
Cada criterio se califica en uno de cinco niveles: Excepcional (5,0), Completo (4,3), Aceptable (3,5),
Parcial(2,5)eInsuficiente(1,0).Lanotaindividualsecalculaasí:
notaindividual=(notadeequipo+bonificación)×factorindividual
Por ejemplo, un equipo con todos los criterios en el nivel Completo y la bonificación de MLlib obtiene
(4,3+0,5)=4,8comonotadeequipo;unintegranteconfactor0,85obtiene4,08.
Calificaciones de cero
• La no entrega dentro del plazo produce 0,0 en la entrega técnica para todoslosintegrantesdel
equipo.
• Un criterio sin evidencia evaluable en los entregables recibidos se califica con 0,0. El nivel
Insuficiente(1,0)correspondeatrabajopresentadoperodeficiente.
• Laautoríanoacreditadaproduce0,0enlaentregatécnicaparaelintegrante(secciónXII).
• Lainasistenciainjustificadaalasustentaciónproduce0,0enlasustentaciónoral(AnexoC).
Extensión del informe
Si el informe de consultoría supera una página, el criterio Informe y anexos se califica como máximo
enelnivelParcial.
BigData2026-II·Casodeestudio·Página12

ANEXO A. RÚBRICAS DE EVALUACIÓN
Criterio de asignación de nivel. Cada descriptor agrupa varios elementos para caracterizar el desempeño típico de ese nivel; no se exige
que todos estén presentes para asignarlo. El nivel se asigna al descriptor que mejor representa el desempeño global en el criterio. Cuando
el trabajo presenta elementos de dos niveles adyacentes, se asigna el nivel inferior si el elemento faltante es central al criterio y el superior
siesperiférico.Uncriteriosinevidenciaevaluablesecalificacon0,0(secciónXIV).
A.1. Entrega técnica (nota de equipo)
Criterio Excepcional(5,0) Completo(4,3) Aceptable(3,5) Parcial(2,5) Insuficiente(1,0)
Selección Elconjuntotieneentre2y5GB Elconjuntocumplelos Elconjuntocumple Elconjuntonocumplealgún Elconjuntonocumplelos
delconjunto descomprimido,provienede requisitosdetamaño, parcialmenteloscriterios,con requisitoimportantede requisitosmínimos:
dedatos unafuenteoficial,suúltima actualización,coberturay unadesviaciónmáximadel tamaño,actualización, provienedefuentesno
(10%) actualizaciónesposterioral1 fuenteoficial.Esrelevante 10%entamaño.Tiene coberturaotipodefuente.La confiablesonocumplelas
dejuliode2026ysucobertura paraelprogramayla relevanciamoderadayla justificaciónesdébilosin fechasestablecidas.No
incluyealmenosunmesde justificaciónessatisfactoria. justificaciónessuperficialo conexiónclaraconel hayjustificaciónadecuada
2026.Esaltamenterelevante Lasreferenciasestán genérica.Lacitapresenta programa.Lacitaes nireferenciasverificables.
paraCienciadeDatosysu correctamentecitadas.La erroresmenoresdeformato. incorrectaoincompleta.
elecciónsejustificaconclaridad propuestaseaprobócon Laaprobaciónrequirióvarios
apartirdelproblemaque ajustesmenores. envíos.
abordaparalaentidad.Las
referenciasestánenformato
APA7.Lapropuestaseaprobó
enelprimerenvío.
Calidaddel Elobjetivoestábiendefinidoy Elobjetivoesclaroyla Elobjetivoesidentificableyla Elobjetivoespococlaroola Elanálisiscarecederigor
análisisy alineadoconlasnecesidades metodologíaestábien metodologíasigueuna metodologíaestáincompleta, metodológico.Los
resultados delaentidadcontratante.La estructurada,conalmenosdos estructurabásica,conal conmenosdedos resultadossonconfusosy
(25%) metodologíadescribecon transformacionescomplejas menosdostransformaciones transformacionescomplejas. lasvisualizaciones
claridadlospasosdel correctamentejustificadas.Los complejas.Losresultadosson Losresultadosmuestran inadecuadas.Se
procesamientoylas resultadossepresentancon comprensibles,conalmenos inconsistenciasofaltade evidenciacomprensión
transformacionescomplejas claridad,convisualizaciones unavisualizaciónadecuada. claridad.Lasvisualizaciones deficientedelosdatos.
aplicadas,consujustificación. apropiadasquecomplementan Semuestraentendimiento sonbásicasonoapoyanel Lasconclusionesson
Losresultadosaportan elanálisis.Seevidenciabuena generaldelosdatos,aunque análisis.Lacomprensiónde débilesonoserelacionan
hallazgosvaliosos,con comprensióndelosdatos.Las faltaprofundidad.Las losdatoseslimitadaylas conelanálisis,ylas
visualizacionesefectivasque conclusionesestán conclusionessoncoherentes, conclusionesnoestán recomendacionesson
apoyanlanarrativa,paletas respaldadasporelanálisisy peropodríanestarmejor completamenterespaldadas inadecuadas.
accesiblesyformatonumérico lasrecomendacionesson sustentadas;las porlaevidencia.
colombiano.Lasconclusiones pertinentes,aunquepodrían recomendacionesson
sefundamentanenevidencia sermásespecíficas. genéricasperoválidas.
cuantitativa,las
recomendacionesson
accionablesyespecíficas,yse
identificanlaslimitacionesdel
análisis.
Usode LosdatosestánenHDFScon LosdatosestánenHDFScon LosdatosestánenHDFS,con LosdatosestánenHDFS, Sparknoseutiliza
BigData2026-II·Casodeestudio·Página13

Criterio Excepcional(5,0) Completo(4,3) Aceptable(3,5) Parcial(2,5) Insuficiente(1,0)
tecnologías replicación2verificable(C1a replicación2ytamaño inconsistenciasmenoresenla peroconproblemasde correctamenteola
deBigData C3),condistribucióndebloques verificable,yladocumentación replicación,yladocumentación replicaciónosinverificación implementaciónfalló.Los
(45%) porDataNode,tamaño debloques,réplicasyestado debloquesynodosesbásica. claradeltamaño.La datosnosecargarona
confirmadoyestadodelos delosnodosescompleta.La Laaplicaciónseejecutócon documentaciónesincompleta HDFSoHDFSnoseusó
nodos.LaSparkSessionse SparkSessionseejecutósobre erroresmenoresresueltos.Se ocontieneerrores.La paraelprocesamiento.No
ejecutósobreYARN,conel YARNconapplication_id documentaalmenosunstage aplicaciónseejecutócon haymétricasdeYARNni
application_iddelaprimera documentado.Sedocumentan relevanteconsusmétricas, erroressignificativosque delaSparkUI.Elcódigo
ejecucióndel9denoviembre dosstagesrelevantesconsus conunajustificaciónsuperficial afectaronlosresultados.Las noesejecutableono
(C4)yunaconfiguración métricas,aunquela delcuellodebotella.Elplan métricassonincorrectaso procesaelconjunto
justificada(C5).Elplan justificacióndesurelevancia de.explain()noseinterpretao incompletas,yelanálisisde completo.Nohay
de.explain()seinterpreta eslimitada.Elplande.explain() carecedelanotasobreAQE. stagesesmuysuperficial.El comandosdeejecución
correctamente,conlanota seinterpretaadecuadamente. Elcódigoesfuncional,con códigopresentaproblemasde documentados.
sobreAQE,isFinalPlan=falsey Elcódigoesfuncional,con documentaciónlimitada,yla ejecuciónonoaprovechalas
sucontrasteconelplanfinal comentariosbásicos,yla configuracióndela capacidadesdistribuidasde
(C6,C7).Sedocumentandos configuracióndela SparkSessionnoestá Spark.Nohayjustificaciónde
stagesrelevantesconsus SparkSessiontienealgunas justificada. laconfiguración.
métricasylajustificacióndesu justificaciones.Lahojade
relevancia(C8,C9).Elcódigo registroindicalafuenteenla
estábienestructuradoy mayoríadelosvalores.
comentadoyprocesael
conjuntocompletocon
transformacionesdistribuidas.
Ladecisiónsobrecacheo
persistenciaestájustificada,
constoragelevelexplícitosise
aplica.Lasoptimizacionesse
aplicansolodonde
corresponden.Lahojade
registroestácompleta,conla
fuentedecadavalor.
Informey Elinformerespetaellímitede Elinformecumpleelformato Elinformetieneunaestructura Elinformeexcedelapáginao Elinformeestá
anexos unapáginayelformato deunapágina,conestructura básicaidentificableyalgunos suestructuraespococlara, desorganizado,con
(20%) establecido,sinerrores lógicay,alosumo,errores erroresderedacción.Las conerroresderedacción erroresexcesivosysin
ortográficos,conestructura ortográficosmenores.Los referenciaspresentanerrores frecuentes.Lasreferencias estructuralógica.Las
lógica(contexto,objetivo, anexosestánorganizados,con menoresdeformato.Los sonincorrectasoincompletas. referenciasson
metodologíaresumida, lascapturaspresentesysu anexosestáncompletos,pero Faltanvariascapturas inadecuadas.Losanexos
resultadosclave,conclusiones análisisenlamayoríadelas desorganizados,oalgunas obligatoriasonocumplenel nocontienenlascapturas
yrecomendaciones)y secciones.Larecomendación capturasnocumplenelcriterio criteriodesuficiencia.La querespaldanlos
redacciónconcisade cloudvs.on-premiseseapoya desuficiencia.La recomendacióncloudvs.on- requerimientos.La
consultoríaejecutiva.Los enalmenosunvalor recomendacióncloudvs.on- premisenopresentauna declaracióndeusodeIA
anexossiguenlaplantilla:todas observado.Ladeclaraciónde premiseesgenérica,sin recomendación.La carecedelatablapor
lascapturasobligatoriasson usodeIAestácompleta.Las valoresobservados.La declaracióndeusodeIAestá componenteodelas
legibles,cumplenelcriteriode referenciasestán declaracióndeusodeIAtiene incompleta. firmas.
suficienciaytienensuanálisis; correctamentecitadas. camposincompletos.
loscomandosestán
documentados.La
recomendacióncloudvs.on-
BigData2026-II·Casodeestudio·Página14

Criterio Excepcional(5,0) Completo(4,3) Aceptable(3,5) Parcial(2,5) Insuficiente(1,0)
premisesefundamentaenal
menosunvalordelclúster
propioyunodelaprácticade
BigQuery.Ladeclaraciónde
usodeIAestácompletayes
consistenteconlabitácora.Las
referenciasestánenformato
APA7.Lapresentaciónestá
organizadaporcomponentes.
A.2. Factor individual
Nivel Descriptor Evidenciaquesecontrasta
1,0 Contribuciónplena:laevidenciadocumentalylasustentaciónson Entradasdelabitácoraenelcomponentedeclaradocomo
consistentesentresíyconladeclaracióndeusodeIA. propio,conevidenciaverificable;respuestassobreese
componenteenlasustentación.
0,85 Contribuciónparcial,oinconsistenciaentrelodeclaradoylosustentado Entradasescasasosinevidenciaverificable,orespuestas
enalgunodeloscomponentes. quenocorrespondenaloregistradoodeclarado.
0,7 Contribuciónmarginal,verificableperomínima. Entradasaisladasodealcancemenorenlabitácora.
Autoríanoacreditada: Seresuelveantesdelfactor.Exigedeformaconcurrentelaausenciade Bitácorasinentradasdelintegranteysustentaciónsin
0,0enlaentrega evidenciadocumentaldecontribuciónylaimposibilidaddesustentarlos respuestasobresucomponente.
técnica componentesdeclaradoscomopropios.
A.3. Sustentación oral (individual)
Criterio Excepcional(5,0) Completo(4,3) Aceptable(3,5) Parcial(2,5) Insuficiente(1,0)
Laexposicióndelcomponente Laexposiciónes Laexposicióntieneuna Laexposiciónes Nolograexplicarel
asignadoesestructuradayse estructuradayclara.Usa estructuraidentificableyes desorganizadaonose componenteasignado,
Segmento ajustaaltiempo.Laexplicaciónes valoresobservadosconuna comprensible,perogeneral. ajustaaltiempo.Presenta presentaerrores
expuesto clarayprecisa,ycadaafirmación interpretación Elusodevalores afirmacionessinrespaldo conceptualesgravesono
(40%) sesustentaenvaloresobservados mayoritariamentecorrecta, observadoseslimitadoosu envaloresobservadoso relacionalaexposicióncon
delaejecuciónpropia, conimprecisionesmenores. interpretaciónpresenta erroresconceptuales. laejecuciónpropia.
interpretadoscorrectamente. erroresmenores.
Respondeconprecisión,ubicael Respondecorrectamente, Respondeparcialmente: Larespuestaesimprecisa Noresponde,olarespuesta
Respuesta
valorobservadoenlos conunajustificacióntécnica identificaelvalor,perola ynorelacionaelvalorcon evidenciadesconocimiento
sobreel
entregables,explicasuorigeny adecuadaeimprecisiones justificaciónessuperficial. suorigenniconla delcomponentedeclarado
componente
justificaladecisióntécnica menores. decisióntécnica. comopropio.
propio(35%)
asociada.
BigData2026-II·Casodeestudio·Página15

Criterio Excepcional(5,0) Completo(4,3) Aceptable(3,5) Parcial(2,5) Insuficiente(1,0)
Respuesta Explicaconprecisiónloesencial Explicacorrectamentelo Explicaelcomponentede Laexplicaciónes Noresponde,olarespuesta
sobreun delcomponente,surelaciónconel esencialdelcomponente, formageneral,sinprecisarel imprecisaocontiene evidenciadesconocimiento
componente propioyelsignificadodelos conimprecisionesmenores. significadodelosvalores. erroresconceptuales. deltrabajodelequipo.
ajeno(25%) valoresporlosquesepregunta.
BigData2026-II·Casodeestudio·Página16

| ANEXO | B. BITÁCORA | DE EQUIPO |     |     |     |
| ----- | ----------- | --------- | --- | --- | --- |
LaplantilladebitácorasepublicaenTeamscomoarchivoeditable.Suestructuraeslasiguiente:
| Sección | Contenido |     |     |     |     |
| ------- | --------- | --- | --- | --- | --- |
Grupo,integrantes,númerodelequipofísicodelasala,integrantepropietariodelclúster
1.Datosdel
dereferencia,URLdelrespaldoennubeyespaciolibredelequipofísicoantesdela
equipo
sesión1.
2.Asignaciónde Integranteresponsabledecadacomponente(A,B,CyD)eintegrantesdeapoyo.
componentes
Unatablapormomentodeltrabajo:S0(preparaciónpreviaenColab),S1(9de
noviembre),S2(11denoviembre),S3(13denoviembre)yTI(trabajoindependientedel
3.Registrode
14al16denoviembre).Cadaentradaregistralafechayhora,elintegrante,el
actividades
componente,laactividadrealizada,laevidenciaverificable(application_id,celda,
capturaocomando)yelusodeIA(herramientaycategoría).
4.Aplicaciones Losapplication_idgeneradosencadasesión,conunadescripciónbreve;elprimerode
| ejecutadas | lasesión1semarcacomoapplication_iddereferencia. |     |     |     |     |
| ---------- | ----------------------------------------------- | --- | --- | --- | --- |
Declaracióndequelosregistroscorrespondenalacontribuciónrealdecadaintegrante,
5.Firmas
connombre,firmayfecha.
La evidencia de cada entrada debe poder verificarse en los entregables: un application_id que
aparezca en el portal de YARN o en las capturas, una celda que exista en el notebook, una captura
incluidaenlosanexosouncomandodocumentado.Unaentradasinevidenciaverificablenoacredita
contribución.
| ANEXO   | C. PROTOCOLO | DE SUSTENTACIÓN |     | ORAL |     |
| ------- | ------------ | --------------- | --- | ---- | --- |
| Franjas | y tiempos    |                 |     |      |     |
La sesión se desarrolla el miércoles 18 de noviembre de 2026, entre las 7:00 y las 11:00 a. m., con
los doce equipos de ambos grupos. El orden de las franjas se define por sorteo y se comunica por
correo electrónico el viernes 13 de noviembre, con la hora de inicio de cada equipo. Entre franjas se
reservandosminutosparaelcambiodeequipo.
| Tramo                       |     |     | Equipodetresintegrantes |     | Equipodedosintegrantes |
| --------------------------- | --- | --- | ----------------------- | --- | ---------------------- |
| Ingresoysorteodecomponentes |     |     | 1min                    |     | 1min                   |
Exposición(unsegmentoporintegrante) 9min(3×3min) 6min(2×3min)
| Preguntasindividuales |     |     | 7,5min(3×2,5min) |     | 5min(2×2,5min) |
| --------------------- | --- | --- | ---------------- | --- | -------------- |
| Cierre                |     |     | 1min             |     | 1min           |
| Total                 |     |     | 18,5min          |     | 13min          |
Desarrollo
1. Elequiposepresentacincominutosantesdesufranjayesperafueradelsalón.Lasustentación
esprivada:enelsalónsoloestáelequipodelafranjaencurso,unadocenteexpertayladocente
de la asignatura. La sustentación se graba en audio con el único fin de respaldar la calificación
y atender las solicitudes de revisión. Cada franja se graba en un archivo independiente, que se
conserva en el almacenamiento institucional de la docente, no se comparte con terceros y se
BigData2026-II·Casodeestudio·Página17

elimina al cierre del período de revisión de calificaciones. Al iniciar la grabación, la docente
informaráalosintegrantesquelasesiónseestágrabando.
2. La docente proyecta la presentación entregada el 17 de noviembre. No se usan otros archivos,
memoriasUSBniversionesmodificadas.Losintegrantesnousandispositivosnimaterialpropio;
cuando una pregunta lo requiere, la docente proyecta los anexos, la hoja de registro o el
notebookentregados.
3. Al inicio de la franja, la docente asigna por sorteo el componente (A, B o C) que expone cada
integrante. En los equipos de dos integrantes, el sorteo asigna dos de los tres componentes; el
componentenoexpuestopuedeserobjetodelaspreguntas.
4. Cada integrante expone el componente asignado con las diapositivas correspondientes. Al
cumplirseeltiempodelsegmento,laexposiciónpasaalsiguienteintegrante.
5. Concluida la exposición, cada integrante responde preguntas individuales: una sobre el
componente que declaró como propio en la bitácora y otra sobre un componente ajeno elegido
porladocente.Laspreguntasseanclanenvaloresobservadosdelaejecuciónpropia(capturas,
hojaderegistroynotebook).Cadaintegranterespondesinayudadesuscompañeros.
6. LadocenteregistralacalificacióndecadaintegranteconlarúbricaA.3yconfirmaomodificael
nivelprovisionaldelfactorindividual.Larúbricaaplicadasearchivaconfecha.
Situaciones especiales
• Llegada después del sorteo: el integrante pierde su segmento de exposición, que se califica
con0,0,perorespondesuspreguntassillegadentrodelafranja.
• Inasistencia injustificada: 0,0 en la sustentación oral. Para efectos del factor individual, la
inasistencia equivale a no sustentar los componentes declarados como propios: si la bitácora
no registra evidencia de contribución del integrante, se configura la autoría no acreditada; en
casocontrario,elnivelprovisionaldelfactorquedacomodefinitivo.
• Inasistencia justificada, conforme al reglamento estudiantil: sustentación supletoria individual
dentrodelasemanadeexámenes,enlafechaquefijeladocente.
• Equipo sin entrega técnica: asiste en su franja; el segmento expuesto se califica con 0,0 y
cada integrante responde preguntas individuales sobre el trabajo desarrollado en las sesiones
del9,11y13denoviembre.
BigData2026-II·Casodeestudio·Página18