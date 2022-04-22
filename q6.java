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

public class q6 {
    public static void main(String[] args) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");

        // arquivo de saida
        Path output = new Path("output/q6");
        Job j = new Job(c, "q6");

        j.setJarByClass(q6.class);
        j.setReducerClass(Reduceq6.class);
        j.setMapperClass(Mapq6.class);
        j.setCombinerClass(Combineq6.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(q6Writable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(false);
    }

    public static class Mapq6 extends Mapper<LongWritable, Text, Text, q6Writable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();
            //ignora o cabeçalho
            if(linha.startsWith("country_or_area")) return;

            String[] colunas = linha.split(";");

            //pega as colunas unidade, ano e preço
            String unit = colunas[7];
            String ano = colunas[1];
            String price = colunas[5];

            //write das chaves sendo unidade e ano com o valor sendo um novo objeto com o numero da ocorrencia (1), o preço,
            //valor minimo e valor maximo
            con.write(new Text(unit + " " + ano), new q6Writable(1, Double.parseDouble(price), 0, 0));

        }
    }

    public static class Combineq6 extends Reducer<Text, q6Writable, Text, q6Writable> {
        public void reduce(Text key, Iterable<q6Writable> values, Context con)
                throws IOException, InterruptedException {
            // Faz uma pré soma ja comparando os valores dos preços
            // afim de encontrar os valores minimos e maximos

            int soma = 0;
            int n = 0;
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;
            for (q6Writable i : values) {
                if (min > i.getSoma()) {
                    min = i.getSoma();
                }
                if (max < i.getSoma()) {
                    max = i.getSoma();
                }
                soma += i.getSoma();
                n += i.getN();

            }
            con.write(key, new q6Writable(n, soma, min, max));
        }
    }


    public static class Reduceq6 extends Reducer<Text, q6Writable, Text, Text> {
        public void reduce(Text key, Iterable<q6Writable> values, Context con)
                throws IOException, InterruptedException {
            /*
            Faz as devidas somas comparando os preços aos valores
            minimo e maximo
             */
            double maximo = Double.MIN_VALUE;
            double minimo = Double.MAX_VALUE;
            int soma = 0;
            int n = 0;
            for (q6Writable i : values) {
                soma += i.getSoma();
                n += i.getN();
                if(maximo<i.getMax()) {
                    maximo = i.getMax();
                }
                if(minimo>i.getMin()) {
                    minimo = i.getMin();
                }
            }
            //write com maximo, minimo e a media
            con.write(key, new Text(maximo + " " + minimo + " " + soma / n));
        }

    }
}
