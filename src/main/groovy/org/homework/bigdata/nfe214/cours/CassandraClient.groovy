package org.homework.bigdata.nfe214.cours

import com.datastax.driver.core.Session
import org.homework.bigdata.nfe214.CassandraConnector
import org.homework.bigdata.nfe214.KeyspaceRepository
import org.homework.bigdata.nfe214.cours.columnfamily.Artists
import org.homework.bigdata.nfe214.cours.columnfamily.Movies
import org.homework.bigdata.nfe214.cours.model.Artist
import org.homework.bigdata.nfe214.cours.model.Movie

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
        CassandraConnector connector = new CassandraConnector()
        connector.connect("127.0.0.1", null)
        Session session = connector.getSession()

        def kRepo = new KeyspaceRepository(session: session)
        //kRepo.createTable("movie", "SimpleStrategy", 3)
        //kRepo.describes()
        kRepo.describe("movie")

        kRepo.use("movie")
        Artists.createTable(session)
        /*Inspection.insertFromJSON(session, """
            {
                 "id": "a1",
                 "last_name": "Coppola",
                 "first_name": "Sofia",
                 "birth_date": "1971"
             }
        """)*/

        Artist.createType(session)
        Movies.createTable(session)
        Movie.insertFromJSON(session, """
            {
                "id": "movie:1",
                "title": "Vertigo",
                "year": 1958,
                "genre": "drama",
                "country": "USA",
                "director": {
                    "id": "artist:3",
                    "last_name": "Hitchcock",
                    "first_name": "Alfred",
                    "birth_date": "1899"
                }
            }
        """)
    }


}
