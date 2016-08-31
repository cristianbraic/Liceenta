package git.scala

/**
  * Created by Cristian on 30.08.2016.
  */
object Regex {
  var file = "J:\\Facultate\\taxi_february.txt"
  def main(args: Array[String]): Unit = {
    import java.io._
    import scala.io.Source

    val lines = Source.fromFile(file) // use fromFile here
    val regex1 = ";".r
    val regex3 = "\\)".r
    val regex4 = "\\..*\\(".r

    val pw = new PrintWriter(new File("J:\\Facultate\\taxi_february_remastered.txt" ))

    for (line <- lines.getLines) {
      if (line != "") {
        var processedLine = regex4.replaceAllIn(line, ",")
        processedLine = regex1.replaceAllIn(processedLine, ",")
        processedLine = regex3.replaceAllIn(processedLine, "")
        processedLine = processedLine.patch(processedLine.lastIndexOf(' '), ",", 1)
        pw.println(processedLine)
      }
    }

    pw.close
  }
}
