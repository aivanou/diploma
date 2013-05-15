/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.dip.data.transfer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author alex
 */
public class HBaseDataHandler {

    private final String table;
    private HTable htable;
    private Scan scan;
    private ResultScanner scanner;

    public HBaseDataHandler(String table) throws IOException {
        this.table = table;
        init();
    }

    private void init() throws IOException {
        htable = new HTable(table);
        scan = new Scan();
        scan.addFamily(Bytes.toBytes("p"));
        scan.addFamily(Bytes.toBytes("f"));
    }

    public void open() throws IOException {
        scanner = htable.getScanner(scan);
    }

    public void close() {
        scanner.close();
    }

    public Collection<CrawlerData> readPortion(int size) throws IOException {
        Collection<CrawlerData> data = new ArrayList<CrawlerData>(size);
        int index = 0;
        while (true) {
            Result res = scanner.next();
            if (res == null || index == size) {
                break;
            }
            List<KeyValue> contentCols = res.getColumn(Bytes.toBytes("p"), Bytes.toBytes("c"));
            List<KeyValue> titleCols = res.getColumn(Bytes.toBytes("p"), Bytes.toBytes("t"));
            List<KeyValue> urlCols = res.getColumn(Bytes.toBytes("f"), Bytes.toBytes("bas"));
            String content = getValue(contentCols);
            String title = getValue(titleCols);
            String url = getValue(urlCols);
            if (url.isEmpty() || title.isEmpty() || content.isEmpty()) {
                continue;
            }
            CrawlerData cdata = new CrawlerData(url, content, title);
            data.add(cdata);
            index += 1;
        }
        return data;
    }

    private String getValue(List<KeyValue> kvs) {
        return kvs.isEmpty() ? "" : Bytes.toString(kvs.listIterator().next().getValue());
    }

    public static void main(String[] args) throws IOException {
        HBaseDataHandler handler = new HBaseDataHandler("crawlerdata");
        handler.open();
        Collection<CrawlerData> data = handler.readPortion(1000);
        handler.close();
        for (CrawlerData cdata : data) {
            System.out.println(cdata);
        }
    }
}
