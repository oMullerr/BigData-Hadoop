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

public class q5 {
    public static void main(String[] args) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");

        // arquivo de saida
        Path output = new Path("output/q5");
        Job j = new Job(c, "q5");

        j.setJarByClass(q5.class);
        j.setMapperClass(Mapq5.class);
        j.setReducerClass(Reduceq5.class);

        j.setCombinerClass(Combineq5.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(Commodity.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(false);
    }

    public static class Mapq5 extends Mapper<LongWritable, Text, Text, Commodity> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();
            // ignora o cabeçalho
            if(linha.startsWith("country_or_area")) return;
            String[] colunas = linha.split(";");
            //pega as colunas flow type, pais, unit, categoria e preço
            String flow = colunas[4];
            String pais = colunas[0];
            String unit = colunas[7];
            String category = colunas[9];
            String ano = colunas[1];
            String price = colunas[5];

            // condição que filtra para que o pais seja Brazil e o flow type seja Export
            if (pais.compareTo("Brazil") == 0 && flow.compareTo("Export") == 0) {
                //write das chaves e o valor sendo um objeto com um n de ocorrencias e um valor de preço das da coluna
                con.write(new Text(unit + " " + ano + " " + category + " " + flow + " " + pais), new Commodity(1, Double.parseDouble(price)));

            }
        }
    }


    public static class Combineq5 extends Reducer<Text, q6Writable, Text, Commodity> {
        public void reduce(Text key, Iterable<Commodity> values, Context con)
                throws IOException, InterruptedException {
            //O combine faz a soma feita no reduce com antecedencia para poupar tempo
            int soma = 0;
            int n = 0;
            for (Commodity i : values) {
                soma += i.getSoma();
                n += i.getN();
            }
            con.write(key, new Commodity(n, soma));
        }
    }


    public static class Reduceq5 extends Reducer<Text, Commodity, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<Commodity> values, Context con)
                throws IOException, InterruptedException {
            // Somando as vezes que algumas transação ocorreu
            int soma = 0;
            int n = 0;
            for (Commodity i : values) {
                soma += i.getSoma();
                n += i.getN();
            }
            con.write(key, new DoubleWritable((double) soma / n));
        }

    }
}
