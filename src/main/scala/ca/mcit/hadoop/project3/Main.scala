package ca.mcit.hadoop.project3

import java.io._
import scala.io.Source
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream,FSDataOutputStream,FileSystem, Path}

object Main extends App {

  //******************************//
  /* Set up HDFS environment */
  //*****************************//
  val path:Path=new Path("/user/fall2019/sappu")
  val src:Path=new Path("/user/fall2019/sappu/stm")
  val dest:Path=new Path("/user/fall2019/sappu/course3")
  val conf = new Configuration()
  conf.addResource(new Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
  conf.addResource(new Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))
  var fileSystem:FileSystem = FileSystem.get(conf)
  println(fileSystem.getUri)
  println( fileSystem.getWorkingDirectory)
  if(fileSystem.exists(path)){
    println("Directory:sappu exists")
  }
  else{
    println("Directory:sappu does not exists")
  }
  if (fileSystem.exists(path)){
    println("deleting file : " + path)
    fileSystem.delete(path,true)
  }
  else {
    println("File/Directory" + path + " does not exist")
  }
  fileSystem.mkdirs(path)
  if(fileSystem.exists(path)){
    println("Directory:sappu successfully created")
  }
  fileSystem.mkdirs(src)
  println("stm directory created")
fileSystem.mkdirs(dest)
  println("course3 directory created")
  fileSystem.copyFromLocalFile(new Path("/home/bd-user/Downloads/trips.txt"),src)
  fileSystem.copyFromLocalFile(new Path("/home/bd-user/Downloads/routes.txt"),src)
  fileSystem.copyFromLocalFile(new Path("/home/bd-user/Downloads/calendar.txt"),src)
  println("File is Successfully Uploaded!\n")

  //  *** Load data from CSV files into scala collections/structures
  //  Create a List from trips.txt

  val tripFilePath = new Path("/user/fall2019/sappu/stm/trips.txt")
  val tripStream:FSDataInputStream = fileSystem.open(tripFilePath)
  val tripList: List[Trip] = Iterator.continually(tripStream.readLine()).takeWhile(_ != null)
    .toList
    .tail
    .map(_.split(",", -1))
    .map(n => Trip(n(0).toInt, n(1), n(2), n(3), n(4).toInt, n(5).toInt, n(6).toInt,
      if (n(7).isEmpty) None else Some(n(7)),
      if (n(8).isEmpty) None else Some(n(8))))
  tripStream.close

  /* Create a List from routes.txt.  Filter for subway services only (route_type =1) */

  val routeFilePath = new Path("/user/fall2019/sappu/stm/routes.txt")
  val routeStream:FSDataInputStream = fileSystem.open(tripFilePath)
  val routeList: List[Route] = Iterator.continually(routeStream.readLine()).takeWhile(_ != null)
    .toList
    .tail
    .map(_.split(",", -1))
    .map(n => Route(n(0).toInt, n(1), n(2), n(3), n(4).toInt, n(5), n(6), n(7)))
    .filter(_.routeType == 1)
  routeStream.close

  /* Create a List from calendar.txt.  Filter for Monday services only */

  val calendarFilePath = new Path("/user/fall2019/sappu/stm/calendar.txt")
  val calendarStream:FSDataInputStream = fileSystem.open(calendarFilePath)
  val calendarList: List[Calendar] = Iterator.continually(calendarStream.readLine()).takeWhile(_ != null)
    .toList
    .tail
    .map(_.split(",", -1))
    .map(n => Calendar(n(0), n(1).toInt, n(2).toInt, n(3).toInt, n(4).toInt, n(5).toInt, n(6).toInt, n(7).toInt, n(8), n(9)))
    .filter(_.monday == 1)
  calendarStream.close

  // ***************************************************************************
  /*  Enrich the Trip data with Route and Calender info
      Join Trips and Routes on the route_ID  using a Map */
  // ***************************************************************************

  val routeMap: RouteLookup = new RouteLookup(routeList)
  val routeTrips: List[RouteTrip] =
    tripList.map(line => RouteTrip(line, routeMap.lookup(line.routeId)))
      .filter(_.route != null)

  /*  Join routeTrips to Calender using a NestedLoopJoin */

  val enrichedTrips: List[JoinOutput] =
    new GenericNestedLoopJoin[RouteTrip, Calendar]((i, j) => i.trip.serviceId == j.serviceId)
      .join(routeTrips, calendarList)

  val outDataLines: List[String] =
    enrichedTrips
      .map(n =>
        EnrichedTrip.formatOutput(n.left.asInstanceOf[RouteTrip].trip,
          n.left.asInstanceOf[RouteTrip].route,
          n.right.asInstanceOf[Calendar]))

  /*  Create the output file */

  fileSystem.delete(new Path("/user/fall2019/sappu/course3/finaloutput.csv"), true)
  val filePath = new Path("/user/fall2019/sappu/course3/finaloutput.csv")
  val outputStream: FSDataOutputStream = fileSystem.create(filePath)
  outputStream.writeChars("route_id, service_id, trip_id, trip_headsign, direction_id, shape_id, wheelchair_accessible, note_fr, note_en, route_long_name")

  for (line <- outDataLines) {
    outputStream.writeChars("\n" + line)
  }
  outputStream.close()

  /*
  /*  display output to console */

printf("%-5s| %-20s| %-35s| %-50s| %-5s| %-5s| %-7s| %-7s| %-20s| %-20s|",
    "Route", "Service", "Trip ID", "Trip Head Sign", "Dir", "Shape", "WChair", "Subway", "Note French", "Note Eng")
  println()

  for (line <- outDataLines) {
    val b: Array[String] = line.split(",", -1)
    printf("%-5s| %-20s| %-35s| %-50s| %-5s| %-5s| %-7s| %-7s| %-20s| %-20s|",
      b(0), b(1), b(2), b(3), b(4), b(5), b(6), b(9), b(7), b(8))
    println()
  }
  println()
  println(s"Data enrichment complete.  ${outDataLines.size} records written to file finaloutput.csv")


   */
}

