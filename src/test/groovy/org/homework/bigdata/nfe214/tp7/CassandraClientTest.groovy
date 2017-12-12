package org.homework.bigdata.nfe214.tp7

import com.datastax.driver.core.Session
import org.homework.bigdata.nfe214.CassandraConnector
import org.homework.bigdata.nfe214.KeyspaceRepository
import spock.lang.Shared
import spock.lang.Specification

/**
 * http://b3d.bdpedia.fr/cassandra_tp.html
 */
class CassandraClientTest extends Specification {

    @Shared
    Session session

    @Shared
    CassandraConnector connector

    @Shared
    KeyspaceRepository repository

    def setupSpec() {
        connector = new CassandraConnector()
        connector.connect("127.0.0.1", 9042)
        session = connector.getSession()
        repository = new KeyspaceRepository(session: session)
        repository.use("resto_NY")
    }

    def cleanupSpec() {
        session.close()
        connector.close()
    }


    def "Liste de tous les restaurants"() {
        when:
        def resultSet = session.execute("select * from restaurant")

        then:
        resultSet != null
        resultSet.each {
            println it
        }
    }

    def "Liste des Noms de restaurants."() {
        when:
        def resultSet = session.execute("select name from restaurant")

        then:
        resultSet != null
        resultSet.each {
            println it
        }
    }

    def "Nom et quartier du restaurant N°41569764"() {
        when:
        def resultSet = session.execute("select name, borough from restaurant where id = 41569764")

        then:
        resultSet != null
        resultSet.each {
            println it
        }
    }

    def "Dates et grades des inspections de ce restaurant du restaurant N°41569764"() {
        when:
        def resultSet = session.execute("select InspectionDate, Grade from Inspection where idRestaurant = 41569764")

        then:
        resultSet != null
        resultSet.each {
            println it
        }
    }

    def "Noms des restaurants de cuisine Française (French)."() {
        when:
        def resultSet = session.execute("select name from restaurant where CuisineType = 'French'")

        then:
        resultSet != null
        resultSet.each {
            println it
        }
    }

    def "Noms des restaurants situés dans BROOKLYN (attribut borough)."() {
        when:
        def resultSet = session.execute("select name from restaurant where borough = 'BROOKLYN' ALLOW FILTERING")

        then:
        resultSet != null
        resultSet.each {
            println it
        }
    }

    def "Grades et scores donnés pour une inspection pour le restaurant n° 41569764 avec un score d’au moins 10"() {
        when:
        def resultSet = session.execute("select grade, score from inspection where idRestaurant=41569764 and score >= 10 ALLOW FILTERING")

        then:
        resultSet != null
        resultSet.each {
            println it
        }
    }

    // Je n'arrivais pas à faire cette requête à cause du nombre de tombstone
    // j'ai fait un alter table Inspection with GC_GRACE_SECONDS=86400 ;
    def "8 - Grades (non nuls) des inspections dont le score est supérieur à 30"() {
        when:
        def resultSet = session.execute("""select grade from inspection where score > 30 and grade > '""' ALLOW FILTERING""")

        then:
        resultSet != null
        resultSet.each {
            println it
        }
    }

    def "9 - Compter le nombre de lignes retournées par la requête précédente."() {
        when:
        def resultSet = session.execute("""select count(*) from inspection where grade > '""' and score > 30 ALLOW FILTERING""")

        then:
        resultSet != null
        resultSet.each {
            println it
        }
    }

    def "10 - Grades des inspections dont l’identifiant est compris entre 40 000 000 et 40 000 100"() {
        when:
        def resultSet = session.execute("""
            select grade 
            from inspection 
            where token(idRestaurant) >= 40000000
            LIMIT 100
        """)

        then:
        resultSet != null
        resultSet.each {
            println it
        }
    }

    def "11 - Compter le nombre de lignes retournées par la requête précédente"() {
        when:
        def resultSet = session.execute("""
            select count(*) 
            from inspection 
            where token(idRestaurant) >= 40000000
            LIMIT 100
        """)

        then:
        resultSet != null
        resultSet.each {
            println it
        }
    }

    /*
    SELECT Name FROM Restaurant WHERE borough='BROOKLYN' ;
     */
    def "12 - Pour la requête ci-dessous faites en sorte qu’elle soit exécutable sans ALLOW FILTERING"() {
        setup:
        session.execute("""
                CREATE INDEX IF NOT EXISTS fk_borough_Restaurant ON Restaurant ( borough ) ;
              """)
        when:
        def resultSet = session.execute("""
            SELECT Name FROM Restaurant WHERE borough='BROOKLYN'
        """)

        then:
        resultSet != null
        resultSet.each {
            println it
        }
    }

    def "13 - Trouvez tous les noms de restaurants français de Brooklyn"() {
        setup:
        session.execute("TRACING ON")
        when:
        def resultSet = session.execute("""
            SELECT Name FROM Restaurant WHERE borough='BROOKLYN' AND CuisineType = 'French' ALLOW FILTERING
        """)

        then:
        resultSet != null
        resultSet.each {
            println it
        }
    }
}
