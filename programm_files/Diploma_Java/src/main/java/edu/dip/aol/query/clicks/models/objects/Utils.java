/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.dip.aol.query.clicks.models.objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @author alex
 */
public class Utils {

    public static void writeString(DataOutput output, String str) throws IOException {
        int length = str.length();
        output.writeInt(length);
        output.writeChars(str);
    }

    public static String readString(DataInput input) throws IOException {
        int stringLength = input.readInt();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < stringLength; i++) {
            builder.append(input.readChar());
        }
        return builder.toString();
    }
}
