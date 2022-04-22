package tde1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
public class q2 {
    public static void main(String[] args) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");

        // arquivo de saida
        Path output = new Path("output/q2");
        Job j = new Job(c, "q2");

        j.setJarByClass(q2.class);
        j.setReducerClass(Reduceq2.class);
        j.setMapperClass(Mapq2.class);
        j.setCombinerClass(Combineq2.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(false);
    }

    public static class Mapq2 extends Mapper<LongWritable, Text, Text, IntWritable> {
        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();

            //ignorando o cabeçalho da base
            if(linha.startsWith("country_or_area")) return;

            String[] colunas = linha.split(";");

            String ano = colunas[1]; // coluna dos anos

            //write por ano, para ver a quantidade de transações por ano
            con.write(new Text(ano), new IntWritable(1));
        }
    }
    public static class Combineq2 extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            //O combine faz a soma feita no reduce com antecedencia para poupar tempo
            int soma = 0;
            for (IntWritable i : values){
                soma += i.get();
            }
            con.write(key, new IntWritable(soma));
        }
    }

    public static class Reduceq2 extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            // Somando as vezes que as transações ocorreram por ano
            int soma = 0;
            for (IntWritable i : values){
                soma += i.get();
            }
            con.write(key, new IntWritable(soma));
        }

    }
}
