/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.dip.aol.query.clicks.models.objects;

/**
 *
 * @author alex
 */
public class RankedUrl implements Cloneable {

	private String url;
	private String query;
	private String session;
	private int rank;
	private int click;
	private float attrDistributionFactor;
	private float satDistributionFactor;
	private float attractivness;
	private float satisfaction;
	private final Object lock = new Object();

	public RankedUrl() {
		this.attrDistributionFactor = 0.5f;
		this.satDistributionFactor = 0.5f;
		this.attractivness = 0.0f;
		this.satisfaction = 0.0f;
	}

	public RankedUrl(String url, int rank, int click) {
		this.attrDistributionFactor = 0.5f;
		this.satDistributionFactor = 0.5f;
		this.attractivness = 0.0f;
		this.satisfaction = 0.0f;
		this.url = url;
		this.rank = rank;
		this.click = click;
	}

	public RankedUrl(String url, String query, String session, int rank,
			int click) {
		this();
		this.url = url;
		this.query = query;
		this.session = session;
		this.rank = rank;
		this.click = click;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public String getSession() {
		return session;
	}

	public void setSession(String session) {
		this.session = session;
	}

	public String getUrl() {
		return url;
	}

	public float getSatisfaction() {
		return satisfaction;
	}

	public void setSatisfaction(float satisfaction) {
		this.satisfaction = satisfaction;
	}

	public float getAttractivness() {
		return attractivness;
	}

	public void setAttractivness(float attractivness) {
		this.attractivness = attractivness;
	}

	public float getAttrDistributionFactor() {
		return attrDistributionFactor;
	}

	public void setAttrDistributionFactor(float attrDistributionFactor) {
		this.attrDistributionFactor = attrDistributionFactor;
	}

	public float getSatDistributionFactor() {
		return satDistributionFactor;
	}

	public void setSatDistributionFactor(float satDistributionFactor) {
		this.satDistributionFactor = satDistributionFactor;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public int getRank() {
		return rank;
	}

	public void setRank(int rank) {
		this.rank = rank;
	}

	public int getClick() {
		return click;
	}

	public void setClick(int click) {
		this.click = click;
	}

	@Override
	protected RankedUrl clone() throws CloneNotSupportedException {
		RankedUrl newRurl = null;
		synchronized (lock) {
			newRurl = new RankedUrl(url, query, session, rank, click);
			newRurl.setAttractivness(attractivness);
			newRurl.setSatisfaction(satisfaction);
			newRurl.setAttrDistributionFactor(attrDistributionFactor);
		}
		return newRurl;
	}

	@Override
	public int hashCode() {
		int hash = 5;
		hash = 97 * hash + (this.url != null ? this.url.hashCode() : 0);
		hash = 97 * hash + (this.query != null ? this.query.hashCode() : 0);
		hash = 97 * hash + (this.session != null ? this.session.hashCode() : 0);
		return hash;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final RankedUrl other = (RankedUrl) obj;
		if ((this.url == null) ? (other.url != null) : !this.url
				.equals(other.url)) {
			return false;
		}
		if ((this.query == null) ? (other.query != null) : !this.query
				.equals(other.query)) {
			return false;
		}
		if ((this.session == null) ? (other.session != null) : !this.session
				.equals(other.session)) {
			return false;
		}
		if (this.rank != other.rank) {
			return false;
		}
		if (this.click != other.click) {
			return false;
		}
		return true;
	}
}
