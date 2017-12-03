package org.homework.bigdata.nfe214.tp7.columnfamily

import com.datastax.driver.core.Session
import org.homework.bigdata.nfe214.cours.model.Artist

class Inspection {

    static void createTable(Session session) {
        session.execute("""
                create table IF NOT EXISTS Inspection (
                    idRestaurant INT, InspectionDate date, ViolationCode VARCHAR,
                    ViolationDescription VARCHAR, CriticalFlag VARCHAR, Score INT, GRADE VARCHAR,
                    PRIMARY KEY ( idRestaurant, InspectionDate )
              );
              """)
        session.execute("""
                CREATE INDEX IF NOT EXISTS fk_Inspection_Restaurant ON Inspection ( Grade ) ;
              """)
    }

    static void insertArtist(Session session, Artist artist) {
        session.execute("insert into artists (id, last_name, first_name, birth_date")
    }

    static void insertFromJSON(Session session, String json) {
        session.execute("""
            insert into artists JSON '$json';
        """
        )

    }
}
