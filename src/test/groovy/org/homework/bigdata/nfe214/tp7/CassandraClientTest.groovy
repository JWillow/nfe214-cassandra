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

    def "2-1-1 - Modele de données"() {
        when:
        session.execute("""
            CREATE TYPE Restaurant (
                Name VARCHAR, borough VARCHAR, BuildingNum VARCHAR, Street VARCHAR,
               ZipCode INT, Phone VARCHAR, CuisineType VARCHAR);
        """)
        session.execute("""
            CREATE TABLE InspectionRestaurant (
               idRestaurant INT, InspectionDate date, ViolationCode VARCHAR,
               ViolationDescription VARCHAR, CriticalFlag VARCHAR, Score INT,
               GRADE VARCHAR, Restaurant frozen<Restaurant>,
              PRIMARY KEY ( idRestaurant, InspectionDate )
             );
        """)

        then:
        assert true
    }

    def "2-1-2 - Insertion de données dans le modèle"() {
        setup:
        session.execute("""
            INSERT INTO InspectionRestaurant JSON ' {"idRestaurant":40373938,
               "restaurant": {"name":"IHOP", "borough":"BRONX", "buildingnum":"5655",
                              "street":"BROADWAY", "zipcode":"10463",
                              "phone":"7185494565", "cuisineType":"American"},
              "inspectionDate":"2016-08-16",
              "violationCode":"04L",
              "violationDescription": "On voit des sourtis!.",
              "criticalFlag": "Critical",
              "score":15,
              "grade":"A"}';
        """)

        when:
        def resultSet = session.execute("Select * from InspectionRestaurant")

        then:
        resultSet != null
        resultSet.each {
            println it
        }
    }

    def "2-1-3 - Création d'un index"() {
        setup:
        session.execute("""
            CREATE INDEX IF NOT EXISTS InspectionRestaurant_grade ON InspectionRestaurant ( Grade ) ;
        """)

        when:
        def resultSet = session.execute("Select Restaurant from InspectionRestaurant where grade='A'")

        then:
        resultSet != null
        resultSet.each {
            println it
        }
    }

    def "2-2-1"() {
        when:
        session.execute("""
            CREATE TYPE Inspection (
              ViolationCode VARCHAR,
              ViolationDescription VARCHAR, CriticalFlag VARCHAR, Score INT, GRADE VARCHAR,
            ) ;
        """)
        session.execute("""
            CREATE TABLE RestaurantInspections (
               id INT, Name VARCHAR, borough VARCHAR, BuildingNum VARCHAR, Street VARCHAR,
               ZipCode INT, Phone VARCHAR, CuisineType VARCHAR,
               Inspections map<text, frozen<Inspection>>,
               PRIMARY KEY (id)
            );
        """)

        then:
        assert true
    }


    def "2-2-2 - Insertion de données dans le modèle"() {
        setup:
        session.execute("""
            INSERT INTO RestaurantInspections JSON ' {"id":40373938,
             "name":"IHOP", "borough":"BRONX", "buildingnum":"5655", "street":"BROADWAY",
             "zipcode":"10463", "phone":"7185494565", "cuisineType":"American",
             "inspections":{
                "2016-08-16":{"violationCode":"04L", "violationDescription":
                  "Evidence of mice.",
                  "criticalFlag":"Critical", "score":15, "grade":""},
                "2014-02-20":{"violationCode":"08C", "violationDescription":
                  "Pesticide used!",
                  "criticalFlag":"Not Critical", "score":7, "grade":""},
                "2014-03-11":{"violationCode":"10B", "violationDescription":
                  "Plumbing not properly installed.",
                  "criticalFlag":"Not Critical", "score":12, "grade":"A"}
              }}';
        """)

        when:
        def resultSet = session.execute("Select * from RestaurantInspections")

        then:
        resultSet != null
        resultSet.each {
            println it
        }
    }

    def "2-2-4 - Trouver tous les restaurants du Bronx"() {
        when:
        def resultSet = session.execute("select * from RestaurantInspections where borough = 'BRONX' allow filtering")

        then:
        assert resultSet != null
        resultSet.each {
            println ">>>> $it"
        }
    }

    def "2-2-5 - Trouver tous les restaurants du Bronx"() {
        when:
        def resultSet = session.execute("select * from RestaurantInspections where borough = 'BRONX' allow filtering")

        then:
        assert resultSet != null
        resultSet.each {
            println ">>>> $it"
        }
    }
    /**
     * CREATE INDEX RestaurantInspections_Grade ON RestaurantInspections ( VALUE(Inspections) ) ;
     */
    def "Create index on MAP"() {
        when:
        session.execute("CREATE INDEX IF NOT EXISTS RestaurantInspections_entries ON RestaurantInspections ( VALUES(Inspections) ) ;")

        def resultSet = session.execute("Select * from RestaurantInspections where inspections CONTAINS '{grade:'A'}")

        then:
        resultSet.each { print ">>>>>>> $it"}
    }
}
