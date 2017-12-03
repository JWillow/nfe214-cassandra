package org.homework.bigdata.nfe214.tp7

import com.datastax.driver.core.Session
import groovy.json.JsonSlurper
import org.homework.bigdata.nfe214.CassandraConnector
import org.homework.bigdata.nfe214.KeyspaceRepository
import org.homework.bigdata.nfe214.tp7.columnfamily.Inspection
import org.homework.bigdata.nfe214.tp7.columnfamily.Restaurant

class CassandraClient {
    // docker run --name remy-cassandra -p 9042:9042 -d cassandra
    // Key Concept
    // - Keyspace : nom de la base de données
    // - Column Family : nom de la table
    // - Column Data structure
    // Pour exécuter les commandes en CQL
    //    - docker exec -i -t remy-cassandra /bin/bash
    //    - cqlsh localhost
    //    - use resto_NY;

    // A frozen value serializes multiple components into a single value.
    // Non-frozen types allow updates to individual fields.
    // Cassandra treats the value of a frozen type as a blob.
    // The entire value must be overwritten.

    static void main(String[] args) {
        JsonSlurper jsonSlurper = new JsonSlurper()

        CassandraConnector connector = new CassandraConnector()
        connector.connect("127.0.0.1", 9042)
        Session session = connector.getSession()

        def kRepo = new KeyspaceRepository(session: session)
        kRepo.create("resto_NY", "SimpleStrategy", 1)
        kRepo.use("resto_NY")

        Restaurant.createTable(session)
        Inspection.createTable(session)
    }


}

