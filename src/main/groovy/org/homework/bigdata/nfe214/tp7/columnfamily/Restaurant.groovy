package org.homework.bigdata.nfe214.tp7.columnfamily

import com.datastax.driver.core.Session

class Restaurant {

    static createTable(Session session) {
        session.execute("""
             CREATE TABLE IF NOT EXISTS Restaurant (
               id INT, Name VARCHAR, borough VARCHAR, BuildingNum VARCHAR, Street VARCHAR,
               ZipCode INT, Phone text, CuisineType VARCHAR,
               PRIMARY KEY ( id )
             ) ;
        """)

        session.execute("""
                CREATE INDEX IF NOT EXISTS fk_Restaurant_cuisine ON Restaurant ( CuisineType ) ;
              """)
    }
}
