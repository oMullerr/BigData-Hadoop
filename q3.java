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
public class q3 {
    public static void main(String[] args) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");

        // arquivo de saida
        Path output = new Path("output/q3");
        Job j = new Job(c, "q3");

        j.setJarByClass(q3.class);
        j.setReducerClass(Reduceq3.class);
        j.setMapperClass(Mapq3.class);
        j.setCombinerClass(Combineq3.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(false);
    }

    public static class Mapq3 extends Mapper<LongWritable, Text, Text, IntWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            // ignora o header
            if(linha.startsWith("country_or_area")) return;

            String[] colunas = linha.split(";");
            // pega as colunas de flow type e ano
            String flowtype = colunas[4]; // tipo da transação (import/export)
            String ano = colunas[1];

            //write com chaves contendo cada ano e cada flowtype, e contando as transações
            con.write(new Text("Ano: " + ano + "| Flow: " + flowtype), new IntWritable(1)); // 'duas chaves' (ano/flowtipe)

        }
    }

    public static class Combineq3 extends Reducer<Text, IntWritable, Text, IntWritable> {
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

    public static class Reduceq3 extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int soma = 0;
            for (IntWritable i : values){
                soma += i.get();
            }
            con.write(key, new IntWritable(soma));
        }

    }
}
