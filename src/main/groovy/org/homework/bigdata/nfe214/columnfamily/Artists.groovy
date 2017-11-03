package org.homework.bigdata.nfe214.columnfamily

import com.datastax.driver.core.Session
import org.homework.bigdata.nfe214.model.Artist

class Artists {

    static void createTable(Session session) {
        session.execute("""
                create table IF NOT EXISTS artists (id text,
                last_name text, first_name text,
                birth_date int, primary key (id)
              );
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
