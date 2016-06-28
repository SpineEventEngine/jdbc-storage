package org.spine3.server.storage.jdbc.query;


import com.google.protobuf.Message;
import org.spine3.server.storage.jdbc.DatabaseException;

public interface Read<M extends Message>{

    M execute() throws DatabaseException;
}
