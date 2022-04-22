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
public class q4 {
    public static void main(String[] args) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");

        // arquivo de saida
        Path output = new Path("output/q4");
        Job j = new Job(c, "q4");

        j.setJarByClass(q4.class);
        j.setReducerClass(Reduceq4.class);
        j.setMapperClass(Mapq4.class);
        j.setCombinerClass(Combineq4.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(Commodity.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(false);
    }

    public static class Mapq4 extends Mapper<LongWritable, Text, Text, Commodity> {


        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();
            // ignorando o cabeçalho
            if(linha.startsWith("country_or_area")) return;
            String[] colunas = linha.split(";");
            //pega as colunas ano e preço pelas transações
            String ano = colunas[1];
            String price = colunas[5];
            //write para cada ano (chave) é passado o preço e 1 para cada transação para fazer a média
            con.write(new Text(ano), new Commodity(1,Double.parseDouble(price)));


        }

    }
    public static class Combineq4 extends Reducer<Text, Commodity, Text, Commodity> {
        public void reduce(Text key, Iterable<Commodity> values, Context con)
                throws IOException, InterruptedException {
            //Aqui é feita uma pré soma das ocorrencias e dos preços (soma = preços)
            int soma = 0;
            int n = 0;
            for (Commodity i : values){
                soma += i.getSoma();
                n += i.getN();
            }
            con.write(key, new Commodity(n,soma));
        }
    }

    public static class Reduceq4 extends Reducer<Text, Commodity, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<Commodity> values, Context con)
                throws IOException, InterruptedException {
            // Somando as vezes que algumas transação ocorreu
            int soma = 0;
            int n = 0;
            for (Commodity i : values){
                soma += i.getSoma();
                n += i.getN();
            }
            double media = soma/n;
            con.write(key, new DoubleWritable(media));
        }

    }
}
