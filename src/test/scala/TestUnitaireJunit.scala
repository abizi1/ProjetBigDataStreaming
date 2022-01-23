import org.junit._
import org.junit.Assert._



class TestUnitaireJunit {

  @Test // Annotation qui indique que la fonction suivante est un test
  def testdivision(): Unit = {

    var valeur_actuelle = HelloWorldBigData.division(15,5)
    var valeur_attendue: Int = 3
    assertEquals("valeur attendue 3",valeur_attendue,valeur_actuelle.toInt)

  }

}
