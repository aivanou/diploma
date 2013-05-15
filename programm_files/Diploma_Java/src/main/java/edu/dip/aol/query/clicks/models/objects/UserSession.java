/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.dip.aol.query.clicks.models.objects;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 *
 * @author alex
 */
public class UserSession {

    private String id;
    private String query;
    private List<RankedUrlWritable> rurls;

    public UserSession(String id, String query) {
        this.id = id;
        this.query = query;
        this.rurls = new CopyOnWriteArrayList<RankedUrlWritable>();
    }

    public UserSession() {
        this.rurls = new CopyOnWriteArrayList<RankedUrlWritable>();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public List<RankedUrlWritable> getRurls() {
        return rurls;
    }

    public void addRurl(RankedUrlWritable rurl) {
        this.rurls.add(rurl);
    }
}
