package org.homework.bigdata.nfe214

import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Session
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class KeyspaceRepository {
    static final Logger LOGGER = LoggerFactory.getLogger(KeyspaceRepository.class)

    Session session

    void create(String name, String replicationStrategy, int numberOfReplicas) {
        session.execute("CREATE KEYSPACE IF NOT EXISTS $name " +
                "WITH REPLICATION = { 'class' : '$replicationStrategy', 'replication_factor': $numberOfReplicas };")
    }

    void use(String name) {
        session.execute("USE $name")
    }

    void remove(String name) {
        session.execute("DROP KEYSPACE $name")
    }

    void describes() {
        // Equivaut en CQLSH à DESCRIBE KEYSPACES
        session.getCluster().getMetadata().getKeyspaces().each {
            LOGGER.info it
        }
    }

    void describe(String name) {
        // Equivaut en CQLSH à DESCRIBE KEYSPACE $name
        LOGGER.info session.getCluster().getMetadata().getKeyspace(name)?.toString()
    }

}
