package edu.dip.aol.query.clicks.models.objects;

public class WeightedUrl {

    private String url;
    private double finalAttractivness;
    private double finalSatisfaction;
    private double finalRelevance;

    public WeightedUrl() {
        this.finalAttractivness = 0.0;
        this.finalSatisfaction = 0.0;
        this.finalRelevance = 0.0;
    }

    public WeightedUrl(String url, double finalAttractivness, double finalSatisfaction, double finalRelevance) {
        this.url = url;
        this.finalAttractivness = finalAttractivness;
        this.finalSatisfaction = finalSatisfaction;
        this.finalRelevance = finalRelevance;
    }

    public String getUrl() {
        return this.url;
    }

    public double getFinalAttractivness() {
        return this.finalAttractivness;
    }

    public double getFinalSatisfaction() {
        return this.finalSatisfaction;
    }

    public double getFinalRelevance() {
        return this.finalRelevance;
    }

    @Override
    public String toString() {
        return this.url;
    }


}
