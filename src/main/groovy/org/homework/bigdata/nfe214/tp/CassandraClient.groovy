package org.homework.bigdata.nfe214.tp

import com.datastax.driver.core.Session
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.homework.bigdata.nfe214.CassandraConnector
import org.homework.bigdata.nfe214.KeyspaceRepository
import org.homework.bigdata.nfe214.cours.columnfamily.Artists
import org.homework.bigdata.nfe214.cours.columnfamily.Movies
import org.homework.bigdata.nfe214.cours.model.Artist
import org.homework.bigdata.nfe214.cours.model.Movie
import org.homework.bigdata.nfe214.tp.model.Metro
import org.homework.bigdata.nfe214.tp.model.Stop

class CassandraClient {
    // docker run --name remy-cassandra -p 9042:9042 -d cassandra
    // Key Concept
    // - Keyspace : nom de la base de données
    // - Column Family : nom de la table
    // - Column Data structure
    // Pour exécuter les commandes en CQL
    //    - docker exec -i -t remy-cassandra /bin/bash
    //    - cqlsh localhost
    //    - use movies;
    //    - select * from artists;

    // A frozen value serializes multiple components into a single value.
    // Non-frozen types allow updates to individual fields.
    // Cassandra treats the value of a frozen type as a blob.
    // The entire value must be overwritten.

    static void main(String[] args) {
        JsonSlurper jsonSlurper = new JsonSlurper()

        def metroLines = jsonSlurper.parse("./src/main/resources/metro-lines.json" as File)
        def metroStops = jsonSlurper.parse("./src/main/resources/metro-stops.json" as File)


        List<Metro> metros = metroLines.collect { line ->
            Metro metro = new Metro()
            metro.color = line.color
            metro.name = line.name.replaceAll(" ", "-").toUpperCase()
            metro.number = line.number
            metro.routeName = line.route_name

            def selectedStop = metroStops.findAll { stop ->
                stop.lines.find {
                    return (it.line.toUpperCase() == metro.name)
                }
            }
            metro.stops.addAll selectedStop.collect { jsonStop ->
                Stop stop = new Stop()
                stop.name = jsonStop.name
                stop.latitude = jsonStop.latitude
                stop.longitude = jsonStop.longitude
                stop.description = jsonStop.description
                stop.position = jsonStop.lines.find {
                    return (it.line.toUpperCase() == metro.name)
                }.position
                return stop
            }
            return metro
        }

        CassandraConnector connector = new CassandraConnector()
        connector.connect("127.0.0.1", null)
        Session session = connector.getSession()

        def kRepo = new KeyspaceRepository(session: session)
        kRepo.create("metros", "SimpleStrategy", 3)
        kRepo.use("metros")
        Stop.createType(session)
        Metro.createTable(session)

        metros.each { Metro metro ->
            String json = JsonOutput.toJson(metro).replaceAll("'", "''")
            println json
            Metro.insertJSON(session, json)
        }


    }


}

