
package Examen

import Examen__.ejercicio1 // Importamos la función definida en Examen__
import utils.TestInit

class ExamenTest1 extends TestInit {

  "ejercicio1" should "filtrar estudiantes con calificación mayor a 8 y ordenarlos descendentemente" in {
    import spark.implicits._

    // Crear un DataFrame de prueba
    val estudiantes = Seq(
      ("Ana", 20, 9.5),
      ("Juan", 22, 7.8),
      ("Luis", 19, 8.2),
      ("Marta", 21, 6.9),
      ("Pedro", 23, 9.0),
      ("Juan", 23, 6.0)
    ).toDF("nombre", "edad", "calificacion")

    // Llamar a la función a probar
    val resultado = ejercicio1(estudiantes)(spark)

    // Validaciones:
    // 1. Comprobamos que el esquema de salida es el esperado
    assert(resultado.schema.fieldNames.sameElements(Array("nombre", "calificacion")))

    // 2. Validamos los datos esperados
    val resultadoEsperado = Seq(
      ("Ana", 9.5),
      ("Pedro", 9.0)
    ).toDF("nombre", "calificacion")

    assert(resultado.collect() === resultadoEsperado.collect
