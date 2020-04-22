
package ca.mcit.hadoop.project3

case class Trip(
                 routeId: Int,
                 serviceId: String,
                 tripId: String,
                 tripHeadSign: String,
                 directionId: Int,
                 shapeId: Int,
                 wheelchairAccessible: Int,
                 noteFr: Option[String],
                 noteEn: Option[String]
               )


