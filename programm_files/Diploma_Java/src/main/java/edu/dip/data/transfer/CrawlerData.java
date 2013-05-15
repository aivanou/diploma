/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.dip.data.transfer;

/**
 *
 * @author alex
 */
public class CrawlerData {

    private String url;
    private String content;
    private String title;

    public CrawlerData(String url, String content, String title) {
        this.url = url;
        this.content = content;
        this.title = title;
    }

    public CrawlerData() {
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return url + "  [" + title + "]  " + content.length() + "   ";
    }
}
