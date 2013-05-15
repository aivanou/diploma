/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.dip.data.transfer;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;

/**
 *
 * @author alex
 */
public class LuceneIndexer implements Indexer<CrawlerData> {

    private IndexWriter writer;
    private Directory dir;

    public LuceneIndexer() {
    }

    @Override
    public void write(Collection<CrawlerData> data) throws IOException {
        for (CrawlerData cdata : data) {
            try {
                writer.addDocument(convert(cdata));
            } catch (IOException ex) {
                Logger.getLogger(LuceneIndexer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        writer.commit();
    }

    public void open(String dirPath) throws IOException {
        File path = new File(dirPath);
        dir = FSDirectory.open(path);
        IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_41, new StandardAnalyzer(Version.LUCENE_41));
        writer = new IndexWriter(dir, conf);
    }

    public void close() throws IOException {
        writer.close();
    }

    private Document convert(CrawlerData cdata) {
        Document doc = new Document();
        Field url = new StringField("url", cdata.getUrl(), Field.Store.YES);
        Field title = new TextField("title", cdata.getUrl(), Field.Store.YES);
        Field content = new TextField("content", cdata.getContent(), Field.Store.YES);
        doc.add(url);
        doc.add(title);
        doc.add(content);
        return doc;
    }

    public static void main(String[] args) throws IOException {
//        transfer();
        read();
    }

    public static void read() throws IOException {
        String dirPath = "/home/alex/study/diploma/project/luceneIndex";
        File path = new File(dirPath);
        Directory dir = FSDirectory.open(path);
        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);
        TermQuery q = new TermQuery(new Term("content", "news"));
        TopDocs docs = searcher.search(q, 20);
        int index = 0;
        for (ScoreDoc doc : docs.scoreDocs) {
            ++index;
            Document resDoc = reader.document(doc.doc);
            Explanation explanation = searcher.explain(q, doc.doc);
            System.out.println(index + " url:  " + resDoc.get("url"));
            System.out.println(explanation);
        }
    }

    public static void transfer() throws IOException {
        HBaseDataHandler dataHandler = new HBaseDataHandler("crawlerdata");
        String directoryPath = "/home/alex/study/diploma/project/luceneIndex";
        Indexer<CrawlerData> lIndexer = new LuceneIndexer();
        lIndexer.open(directoryPath);
        dataHandler.open();
        while (true) {
            Collection<CrawlerData> portion = dataHandler.readPortion(1000);
            lIndexer.write(portion);
            System.out.println("end processing: " + portion.size());
            if (portion.size() != 1000) {
                break;
            }
        }
        lIndexer.close();
        dataHandler.close();
    }
}
