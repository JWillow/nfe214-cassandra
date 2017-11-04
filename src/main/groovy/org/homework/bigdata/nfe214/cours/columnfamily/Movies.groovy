package org.homework.bigdata.nfe214.cours.columnfamily

import com.datastax.driver.core.Session

class Movies {

    static createTable(Session session) {
        session.execute("""
            create table IF NOT EXISTS movies (id text,
                                 title text,
                                 year int,
                                 genre text,
                                 country text,
                                 director frozen<artist>,
                                 primary key (id) );
        """)
    }
}
