/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.dip.aol.query.clicks.models.objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 *
 * @author alex
 */
public class UserSessionWritable implements WritableComparable<UserSessionWritable> {

    private UserSession session;

    public UserSessionWritable() {
        session = new UserSession();
    }

    public UserSession get() {
        return this.session;
    }

    public void set(UserSession session) {
        this.session = new UserSession();
        this.session.setId(session.getId());
        this.session.setQuery(session.getQuery());
        for (RankedUrlWritable rurl : session.getRurls()) {
            this.session.addRurl(rurl);
        }
    }

    public void write(DataOutput out) throws IOException {
        Text.writeString(out, session.getId());
        Text.writeString(out, session.getQuery());
        int rurlsSize = session.getRurls().size();
        out.writeInt(rurlsSize);
        for (RankedUrlWritable rurl : session.getRurls()) {
            rurl.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        session.setId(Text.readString(in));
        session.setQuery(Text.readString(in));
        int urlsSize = in.readInt();
        session.getRurls().clear();
        for (int i = 0; i < urlsSize; i++) {
            RankedUrlWritable rurl = new RankedUrlWritable();
            rurl.readFields(in);
            session.addRurl(rurl);
        }
    }

    public int compareTo(UserSessionWritable o) {
        if (o == null) {
            return 1;
        }
        return session.getId().compareTo(o.get().getId());
    }
}
