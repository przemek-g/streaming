package fr.inria.simulationProcessor.data

import java.sql.Connection
import scala.collection.mutable.HashMap
import java.sql.DriverManager

object ConnectionProvider {
  
  val _connections = new HashMap[String,Connection]()
  
  private def _createConnection(url:String) : Connection = {
    if (url == null) 
      DriverManager.getConnection("jdbc:derby:default-db;create=true")
    else 
      DriverManager.getConnection(url)
  }
  
  def getConnection(url:String) : Connection = {
    // if the key is not present, evaluates the expression in second arg, associates it with the key, returns it
    _connections.getOrElseUpdate(url, _createConnection(url))
  }
}