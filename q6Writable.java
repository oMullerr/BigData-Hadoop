package tde1;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class q6Writable implements WritableComparable<q6Writable> {

    private int N;
    private double soma;
    private double min;
    private double max;


    public q6Writable() {
    }

    public q6Writable(int n, double soma, double min, double max) {
        N = n;
        this.soma = soma;
        this.min = min;
        this.max = max;
    }

    public int getN() {
        return N;
    }

    public void setN(int n) {
        N = n;
    }

    public double getSoma() {
        return soma;
    }

    public void setSoma(double soma) {
        this.soma = soma;
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    @Override
    public int compareTo(q6Writable o) {
        if (this.hashCode() > o.hashCode()) {
            return +1;
        } else if (this.hashCode() < o.hashCode()) {
            return -1;
        }
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(N);
        dataOutput.writeDouble(soma);
        dataOutput.writeDouble(min);
        dataOutput.writeDouble(max);


    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        N = dataInput.readInt();
        soma = dataInput.readDouble();
        min = dataInput.readDouble();
        max = dataInput.readDouble();

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        q6Writable that = (q6Writable) o;
        return N == that.N && Double.compare(that.soma, soma) == 0 && Double.compare(that.min, min) == 0 && Double.compare(that.max, max) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(N, soma, min, max);
    }
}

