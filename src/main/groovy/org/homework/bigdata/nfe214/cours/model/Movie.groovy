package org.homework.bigdata.nfe214.cours.model

import com.datastax.driver.core.Session

class Movie {

    static insertFromJSON(Session session, String json) {
        session.execute("INSERT INTO movies JSON '$json'")
    }
}
