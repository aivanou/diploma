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
public class RankedUrlWritable implements WritableComparable<RankedUrlWritable> {

    private RankedUrl rurl;

    public RankedUrlWritable() {
        this.rurl = new RankedUrl();
    }

    public RankedUrlWritable(RankedUrl rurl) {
        this.rurl = rurl;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(rurl.getUrl());
        out.writeUTF(rurl.getQuery());
        out.writeUTF(rurl.getSession());
        out.writeFloat(rurl.getAttrDistributionFactor());
        out.writeFloat(rurl.getAttractivness());
        out.writeFloat(rurl.getSatisfaction());
        out.writeInt(rurl.getRank());
        out.writeInt(rurl.getClick());
//        Text.writeString(out, rurl.getUrl());
//        Text.writeString(out, rurl.getQuery());
//        Text.writeString(out, rurl.getSession());
//        WritableUtils.writeVInt(out, rurl.getRank());
//        WritableUtils.writeVInt(out, rurl.getClick());
    }

    public void readFields(DataInput in) throws IOException {
        rurl.setUrl(in.readUTF());
        rurl.setQuery(in.readUTF());
        rurl.setSession(in.readUTF());
        rurl.setAttrDistributionFactor(in.readFloat());
        rurl.setAttractivness(in.readFloat());
        rurl.setSatisfaction(in.readFloat());
        rurl.setRank(in.readInt());
        rurl.setClick(in.readInt());
//        rurl.setUrl(Text.readString(in));
//        rurl.setQuery(Text.readString(in));
//        rurl.setSession(Text.readString(in));
//        rurl.setRank(WritableUtils.readVInt(in));
//        rurl.setClick(WritableUtils.readVInt(in));
    }

    @Override
    public int compareTo(RankedUrlWritable o) {
        if (o == null) {
            return 1;
        }
        int url_cmpr = rurl.getUrl().compareTo(o.get().getUrl());
        if (url_cmpr != 0) {
            return url_cmpr;
        }
        int sess = rurl.getSession().compareTo(o.get().getSession());
        if (sess != 0) {
            return sess;
        }
        if (rurl.getRank() == o.get().getRank()) {
            return 0;
        }
        return rurl.getRank() > o.get().getRank() ? 1 : -1;

    }

    public RankedUrl get() {
        return rurl;
    }

    public void set(RankedUrl rurl) {
        this.rurl.setUrl(rurl.getUrl());
        this.rurl.setQuery(rurl.getQuery());
        this.rurl.setSession(rurl.getSession());
        this.rurl.setAttractivness(rurl.getAttractivness());
        this.rurl.setClick(rurl.getClick());
        this.rurl.setAttrDistributionFactor(rurl.getAttrDistributionFactor());
        this.rurl.setSatisfaction(rurl.getSatisfaction());
        this.rurl.setRank(rurl.getRank());
    }

    public static RankedUrlWritable copy(RankedUrlWritable w) throws CloneNotSupportedException {
        RankedUrlWritable nw = new RankedUrlWritable();
        RankedUrl url = w.get().clone();
        nw.set(url);
        return nw;
    }
}
