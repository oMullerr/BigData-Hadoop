package tde1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class q7 {

    public static void main(String[] args) throws IOException,
            ClassNotFoundException,
            InterruptedException {

        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path("in/transactions_amostra.csv");

        Path output = new Path("output/intermediate");
        Job j = new Job(c, "mr1");

        j.setJarByClass(q7.class);
        j.setReducerClass(Reduce1.class);
        j.setMapperClass(Map1.class);
        j.setCombinerClass(Combine1.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(DoubleWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(false);


        Path input2 = new Path("output/intermediate");
        Path output2 = new Path("output/q7");
        Job j2 = new Job(c, "mr2");

        j2.setJarByClass(q7.class);
        j2.setReducerClass(Reduce2.class);
        j2.setCombinerClass(Combine2.class);
        j2.setMapperClass(Map2.class);


        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(TDE7.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(TDE7.class);

        FileInputFormat.addInputPath(j2, output);
        FileOutputFormat.setOutputPath(j2, output2);

        j2.waitForCompletion(false);

    }

    public static class Map1 extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();

            //ignorando cabeçalho
            if(linha.startsWith("country_or_area")) return;

            String[] colunas = linha.split(";");

            String commcode = colunas[2];
            String amount = colunas[8];
            String flowtype = colunas[4];
            String ano = colunas[1];

            // write apenas quando o ano for 2016
            if(ano.equals("2016")){
                // Caso o ano seja 2016, passamos o comcode, o flowtype e a quantidade.
                con.write(new Text(commcode + "," + flowtype + ","), new DoubleWritable(Double.parseDouble(amount)));
            }
        }
    }

    public static class Combine1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {
            double soma = 0;
            // combine faz uma pré soma dos valores para agilizar
            for (DoubleWritable d : values) {
                soma += d.get();
            }
            con.write(key, new DoubleWritable(soma));
        }
    }
    public static class Reduce1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {
            // soma final das quantidades
            double soma = 0;
            for (DoubleWritable d : values) {
                soma += d.get();
            }
            //write com a chave (commcode e flowtype) e a soma dos amounts de cada chave
            con.write(key, new DoubleWritable(soma));
        }
    }


    public static class Map2 extends Mapper<LongWritable, Text, Text, TDE7> { //maior amount por flowtype
        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();

            linha.replace("\t", ",");
            String[] colunas = linha.split(",");

            String flowtype = colunas[1];
            String commcode = colunas[0];
            double amount = Double.parseDouble(colunas[2]);

            con.write(new Text(flowtype), new TDE7(commcode, amount));
        }
    }

    public static class Combine2 extends Reducer<Text, TDE7, Text, TDE7> {
        public void reduce(Text key, Iterable<TDE7> values, Context con)
                throws IOException, InterruptedException {
            // O combine compara os amounts afim de sempre salvar o maior para agilizar o trabalho do reduce
            String commcode = "";
            double amount = Double.MIN_VALUE;
            for (TDE7 t1 : values) {
                if (t1.getAmount() > amount) {
                    commcode = t1.getCommcode();
                    amount = t1.getAmount();
                }
            }
            con.write(key, new TDE7(commcode, amount));
        }
    }

    public static class Reduce2 extends Reducer<Text, TDE7, Text, TDE7> {
        public void reduce(Text key, Iterable<TDE7> values, Context con)
                throws IOException, InterruptedException {
            String commcode = "";
            double amount = Double.MIN_VALUE;
            // Reduce faz a verificação final dos amounts para garantir que o write seja feito com o maior amount por flowtype
            for (TDE7 t1 : values) {
                if (t1.getAmount() > amount) {
                    commcode = t1.getCommcode();
                    amount = t1.getAmount();
                }
            }
            con.write(key, new TDE7(commcode, amount));
        }
    }
}
