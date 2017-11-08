package converters

import java.io.File

abstract class PlinkFileParser() extends FileParser() {

  def saveParsedFile
  def getFullOutputPath
  def showParserUsage
  
}