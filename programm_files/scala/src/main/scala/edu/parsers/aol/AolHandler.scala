package edu.parsers.aol

import java.text.SimpleDateFormat

class Query(query: String) {

  def getQuery = query

}

class UserAction(val id: Int, val q: Query, val queryTime: Long, val itemRank: Int, val clickUrl: String) {

  def addAction(action: UserAction, actions: List[UserAction]): List[UserAction] = action :: actions

}


class AolHandler {

  def parse(file: String): Unit = {

    var currentId = 0

    def func(line: String) {
      if (!line.trim().toLowerCase.startsWith("anonid")) {
        val action = parseLine(line)
        if (action.id != currentId) {

        }
      }
    }
    def parseLine(line: String): UserAction = {
      val parts = line.split(" +")
      if (parts.length != 5)
        throw new Error("not enough data in a row: [" + line + "]")
      val id = augmentString(parts(0)).toInt
      val query = new Query(parts(1))
      val time = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss").parse(parts(2)).getTime
      val itemRank = augmentString(parts(3)).toInt
      val clickedUrl = parts(4)

      new UserAction(id, query, time, itemRank, clickedUrl)
    }
    scala.io.Source.fromFile(file).getLines().foreach(func)
  }

}
