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
public class q1 {
    public static void main(String[] args) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");

        // arquivo de saida
        Path output = new Path("output/q1");
        Job j = new Job(c, "q1");

        j.setJarByClass(q1.class);
        j.setReducerClass(Reduceq1.class);
        j.setMapperClass(Mapq1.class);
        j.setCombinerClass(Combineq1.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(false);
    }

    public static class Mapq1 extends Mapper<LongWritable, Text, Text, IntWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();

            //ignorando cabeçalho
            if(linha.startsWith("country_or_area")) return;

            String[] colunas = linha.split(";");

            String pais = colunas[0]; //coluna dos países
            // write apenas quando pais for "Brazil"
            if(pais.compareTo("Brazil")==0 ){
                //write com chave brasil e passando um IntWritable(1) que simboliza uma transação de Brazil
                con.write(new Text("Brazil"), new IntWritable(1));
            }
        }
    }

    public static class Combineq1 extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            //O combine faz a soma feita no reduce com antecedencia para poupar tempo
            int soma = 0;
            for (IntWritable i : values){ // passa pela lista de transações do Brasil
                soma += i.get();
            }
            con.write(key, new IntWritable(soma));
        }
    }



    public static class Reduceq1 extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            // Somando as vezes que as transações do Brasil ocorreram
            int soma = 0;
            for (IntWritable i : values){
                soma += i.get();
            }

            con.write(key, new IntWritable(soma));
        }

    }
}
