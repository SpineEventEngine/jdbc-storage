package org.spine3.server.storage.jdbc.query;


import com.google.protobuf.Message;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;

public interface ReadOne<M extends Message>{

    M execute() throws DatabaseException;
}
