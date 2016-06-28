package org.spine3.server.storage.jdbc.query;


import org.spine3.server.storage.jdbc.DatabaseException;

import java.sql.ResultSet;

public interface ReadMany {

    ResultSet execute() throws DatabaseException;
}
