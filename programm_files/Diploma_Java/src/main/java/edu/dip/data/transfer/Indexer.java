/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.dip.data.transfer;

import java.io.IOException;
import java.util.Collection;

/**
 *
 * @author alex
 */
public interface Indexer<T> {

    void write(Collection<T> data) throws IOException;

    void open(String path) throws IOException;

    void close() throws IOException;
}
