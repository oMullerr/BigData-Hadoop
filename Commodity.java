package tde1;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class Commodity implements WritableComparable<q6Writable> {

    private int N;
    private double soma;

    public Commodity() {
    }

    public Commodity(int N, double soma) {
        this.N = N;
        this.soma = soma;
    }

    public int getN() {
        return N;
    }

    public void setN(int n) {
        N = n;
    }

    public void setSoma(double soma) {
        this.soma = soma;
    }

    public double getSoma() {
        return soma;
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

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        N = dataInput.readInt();
        soma = dataInput.readDouble();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Commodity commodity = (Commodity) o;
        return N == commodity.N && Double.compare(commodity.soma, soma) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(N, soma);
    }
}

