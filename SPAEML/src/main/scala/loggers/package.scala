import org.apache.log4j


package object loggers {

  val EpiQuantLogger: log4j.Logger = log4j.LogManager.getLogger("EpiQuant")
}
