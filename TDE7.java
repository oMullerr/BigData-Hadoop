package tde1;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class TDE7 implements WritableComparable<TDE7> {

    String commcode;
    double amount;

    public TDE7() {
    }

    public TDE7(String commcode, double amount) {
        this.commcode = commcode;
        this.amount = amount;
    }

    public String getCommcode() {
        return commcode;
    }

    public void setCommcode(String commcode) {
        this.commcode = commcode;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    @Override
    public int compareTo(TDE7 tde7) {
        if (this.hashCode() > tde7.hashCode()) {
            return +1;
        } else if (this.hashCode() < tde7.hashCode()) {
            return -1;
        }
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(commcode);
        dataOutput.writeDouble(amount);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        commcode = dataInput.readUTF();
        amount = dataInput.readDouble();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TDE7 tde7 = (TDE7) o;
        return Double.compare(tde7.amount, amount) == 0 && Objects.equals(commcode, tde7.commcode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commcode, amount);
    }

    @Override
    public String toString() {
        return "TDE7{" +
                "commcode='" + commcode + '\'' +
                ", amount=" + amount +
                '}';
    }
}
