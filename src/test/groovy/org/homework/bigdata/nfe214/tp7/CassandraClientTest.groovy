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

}
