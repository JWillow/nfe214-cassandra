package org.homework.bigdata.nfe214.tp.model

import com.datastax.driver.core.Session
import groovy.transform.ToString

@ToString
class Metro {
    // primary key
    String number
    String color
    String name
    String routeName
    List<Stop> stops = []

    static createTable(Session session) {
        session.execute("""
            create table IF NOT EXISTS metro (number text,
                                 color text,
                                 name text,
                                 routeName text,                                 
                                 stops frozen<set<stop>>,
                                 primary key (number) );
        """)
    }

    static def insertJSON(Session session, String json) {
        session.execute("INSERT INTO metro JSON '$json'")
    }
}
