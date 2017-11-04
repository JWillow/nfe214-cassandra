package org.homework.bigdata.nfe214.tp.model

import com.datastax.driver.core.Session
import groovy.transform.ToString

@ToString
class Stop {
    String name
    String description
    float latitude
    float longitude
    Integer position

    static createType(Session session) {
        session.execute("""
            create type IF NOT EXISTS stop (
                    name text,
                    description text,
                    latitude float,
                    longitude float,
                    position int);
        """)
    }
}
