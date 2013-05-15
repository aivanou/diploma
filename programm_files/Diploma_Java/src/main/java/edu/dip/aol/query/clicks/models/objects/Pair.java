package edu.dip.aol.query.clicks.models.objects;

public class Pair {

    private double v1;
    private double v2;

    public Pair() {
    }

    public Pair(double v1, double v2) {

        this.v1 = v1;
        this.v2 = v2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Pair pair = (Pair) o;

        if (Double.compare(pair.v1, v1) != 0) return false;
        if (Double.compare(pair.v2, v2) != 0) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(v1);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(v2);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    public double getV1() {

        return v1;
    }

    public void setV1(double v1) {
        this.v1 = v1;
    }

    public double getV2() {
        return v2;
    }

    public void setV2(double v2) {
        this.v2 = v2;
    }
}
