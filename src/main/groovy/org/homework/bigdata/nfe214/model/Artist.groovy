package org.homework.bigdata.nfe214.model

import com.datastax.driver.core.Session

class Artist {
    String id
    String lastName
    String firstName
    int birthDate
    String role;


    static createType(Session session) {
        session.execute("""
            create type IF NOT EXISTS artist (id text,
                    last_name text,
                    first_name text,
                    birth_date int,
                    role text);
        """)
    }
}
