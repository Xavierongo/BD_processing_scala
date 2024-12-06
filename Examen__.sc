
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD



object examen {

  /**Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
   Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
   estudiantes (nombre, edad, calificación).
   Realiza las siguientes operaciones:

   Muestra el esquema del DataFrame.
   Filtra los estudiantes con una calificación mayor a 8.
   Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
   */
  def ejercicio1(estudiantes: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    // Muestra el esquema del DataFrame
    estudiantes.printSchema()

    // Filtra estudiantes con calificación mayor a 8
    val estudiantesFiltrados = estudiantes.filter($"calificacion" > 8)

    // Selecciona nombres y ordenar por calificación descendente
    val estudiantesOrdenados = estudiantesFiltrados
      .select("nombre", "calificacion")
      .orderBy(desc("calificacion"))

    // Muestra el resultado
    estudiantesOrdenados.show()

    // Devuleve el DataFrame como resultado de la consulta
    estudiantesOrdenados
  }


  /** Ejercicio 2: UDF (User Defined Function)
    Pregunta: Define una función que determine si un número es par o impar.
   Aplica esta función a una columna de un DataFrame que contenga una lista de números.
   */

  def ejercicio2(numeros: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    //Definimos la UDF (User Defined Function) que nos permite determinar si un número dado es par o impar.
    val esPar = udf((n: Int) => if (n % 2 == 0) "Par" else "Impar")

    // Creamos una nueva columna "paridad" aplicando la UDF
    val resultado = numeros.withColumn("paridad", esPar($"numero"))

    // Mostramos el DataFrame resultante
    resultado.show()

    // Devolvemos el DataFrame con la nueva columna o atributo
    resultado
  }


  /**Ejercicio 3: Joins y agregaciones
   Pregunta: Dado dos DataFrames,
   uno con información de estudiantes (id, nombre)
   y otro con calificaciones (id_estudiante, asignatura, calificacion),
   realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
   */

def ejercicio3(estudiantes: DataFrame, calificaciones: DataFrame)(implicit spark: SparkSession): DataFrame = {
  import spark.implicits._

  // Ejecuta un join entre estudiantes y calificaciones
  val estudiantesConCalificaciones = estudiantes
    .join(calificaciones, $"id" === $"id_estudiante")
    .select("id", "nombre", "calificacion") // Selección directa por nombre de columnas

  // Calcula la media de  las calificaciones por estudiante
  val promedioPorEstudiante = estudiantesConCalificaciones
    .groupBy("id", "nombre") // Agrupamos por id y nombre del estudiante
    .agg(avg("calificacion").alias("promedio_calificacion")) // Calculamos la media

  // Muestra  el resultado
  promedioPorEstudiante.show()

  // Devuelve el DataFrame con los resultados
  promedioPorEstudiante
}

  /**Ejercicio 4: Uso de RDDs
   Pregunta: Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra.

   */
def ejercicio4(palabras: List[String])(implicit spark: SparkSession): RDD[(String, Int)] = {
  // Crear un RDD (conjunto distribuido de datos) a partir de la lista de palabras.
  val rddPalabras = spark.sparkContext.parallelize(palabras)

  // Cuenta las ocurrencias de cada palabra:
  // 1. `map`: Asocia cada palabra con el número 1 (tupla: palabra -> 1).
  // 2. `reduceByKey`: Suma los valores de las palabras repetidas.
  val ocurrencias = rddPalabras
    .map(palabra => (palabra, 1)) // Crear pares (palabra, 1)
    .reduceByKey(_ + _)           // Agrupar y sumar las ocurrencias

  // Muestra las ocurrencias en consola.
  ocurrencias.collect().foreach(println)

  // Devuelve el RDD con las palabras y sus ocurrencias.
  ocurrencias
}

  /**
     Ejercicio 5: Procesamiento de archivos
     Pregunta: Carga un archivo CSV que contenga información sobre
     ventas (id_venta, id_producto, cantidad, precio_unitario)
    y calcula el ingreso total (cantidad * precio_unitario) por producto.
   */
  def ejercicio5(ventas: String)(spark: SparkSession): DataFrame = {

    // Cargamos el archivo CSV que contiene las ventas.
    val ventas = spark.read.option("header", "true").csv(ventas)

    // Agrupamos los datos por producto y sumamos las cantidades y precios unitarios.
    val agrupacionProductos = ventas.groupBy("id_producto")
      .agg(
        sum("cantidad").alias("TotalCantidad"),
        sum("precio_unitario").alias("TotalPrecioUnitario")
      )

    // Creamos una columna llamada "Total" que multiplica la cantidad por el precio unitario.
    val ingresoTotal = agrupacionProductos.withColumn("Total", col("TotalCantidad") * col("TotalPrecioUnitario"))

    // Mostramos el resultado en la consola.
    ingresoTotal.show()

    // Devolvemos el DataFrame con los resultados.
    ingresoTotal
  }

