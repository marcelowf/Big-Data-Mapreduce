package exercicios;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReduce {

    public static class MapeadorTransacoesBrasil extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable um = new IntWritable(1);
        private Text pais = new Text();

        public void map(Object chave, Text valor, Context contexto) throws IOException, InterruptedException {
            String[] campos = valor.toString().split(",");
            String paisTransacao = campos[0];
            if (paisTransacao.equals("Brasil")) {
                pais.set(paisTransacao);
                contexto.write(pais, um);
            }
        }
    }

    public static class RedutorTransacoesBrasil extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable resultado = new IntWritable();

        public void reduce(Text chave, Iterable<IntWritable> valores, Context contexto) throws IOException, InterruptedException {
            int soma = 0;
            for (IntWritable valor : valores) {
                soma += valor.get();
            }
            resultado.set(soma);
            contexto.write(chave, resultado);

            try {
                FileSystem fs = FileSystem.get(contexto.getConfiguration());
                Path outFile = new Path("output1/resultados.txt");
                FSDataOutputStream out = fs.create(outFile);
                out.writeBytes(chave.toString() + "\t" + resultado.toString() + "\n");
                out.close();
            } catch (IOException e) {
             e.printStackTrace();
            }
        }
    }

    public static class MapeadorTransacoesPorAno extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable um = new IntWritable(1);
        private Text ano = new Text();

        public void map(Object chave, Text valor, Context contexto) throws IOException, InterruptedException {
            String[] campos = valor.toString().split(",");
            String anoTransacao = campos[1];
            ano.set(anoTransacao);
            contexto.write(ano, um);
        }
    }

    public static class RedutorTransacoesPorAno extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable resultado = new IntWritable();

        public void reduce(Text chave, Iterable<IntWritable> valores, Context contexto) throws IOException, InterruptedException {
            int soma = 0;
            for (IntWritable valor : valores) {
                soma += valor.get();
            }
            resultado.set(soma);
            contexto.write(chave, resultado);

            try {
                FileSystem fs = FileSystem.get(contexto.getConfiguration());
                Path outFile = new Path("output2/resultados.txt");
                FSDataOutputStream out = fs.create(outFile);
                out.writeBytes(chave.toString() + "\t" + resultado.toString() + "\n");
                out.close();
            } catch (IOException e) {
             e.printStackTrace();
            }
        }
    }

    public static class MapeadorTransacoesTipoEAno extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable um = new IntWritable(1);
        private Text tipoEAno = new Text();

        public void map(Object chave, Text valor, Context contexto) throws IOException, InterruptedException {
            String[] campos = valor.toString().split(",");
            String tipoFluxo = campos[4];
            String anoTransacao = campos[1];
            tipoEAno.set(tipoFluxo + " " + anoTransacao);
            contexto.write(tipoEAno, um);
        }
    }

    public static class RedutorTransacoesTipoEAno extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable resultado = new IntWritable();

        public void reduce(Text chave, Iterable<IntWritable> valores, Context contexto) throws IOException, InterruptedException {
            int soma = 0;
            for (IntWritable valor : valores) {
                soma += valor.get();
            }
            resultado.set(soma);
            contexto.write(chave, resultado);

            try {
                FileSystem fs = FileSystem.get(contexto.getConfiguration());
                Path outFile = new Path("output3/resultados.txt");
                FSDataOutputStream out = fs.create(outFile);
                out.writeBytes(chave.toString() + "\t" + resultado.toString() + "\n");
                out.close();
            } catch (IOException e) {
             e.printStackTrace();
            }
        }
    }

    public static class MapeadorMediaValoresPorAno extends Mapper<Object, Text, Text, DoubleWritable> {
        private DoubleWritable valor = new DoubleWritable();
        private Text ano = new Text();

        public void map(Object chave, Text texto, Context contexto) throws IOException, InterruptedException {
            String[] campos = texto.toString().split(",");
            String anoTransacao = campos[1];
            double valorTransacao = Double.parseDouble(campos[5]);
            ano.set(anoTransacao);
            valor.set(valorTransacao);
            contexto.write(ano, valor);
        }
    }

    public static class RedutorMediaValoresPorAno extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable resultado = new DoubleWritable();

        public void reduce(Text chave, Iterable<DoubleWritable> valores, Context contexto) throws IOException, InterruptedException {
            double soma = 0.0;
            int contagem = 0;
            for (DoubleWritable valor : valores) {
                soma += valor.get();
                contagem++;
            }
            if (contagem > 0) {
                double media = soma / contagem;
                resultado.set(media);
                contexto.write(chave, resultado);
            }

            try {
                FileSystem fs = FileSystem.get(contexto.getConfiguration());
                Path outFile = new Path("output4/resultados.txt");
                FSDataOutputStream out = fs.create(outFile);
                out.writeBytes(chave.toString() + "\t" + resultado.toString() + "\n");
                out.close();
            } catch (IOException e) {
             e.printStackTrace();
            }
        }
    }

    public static class MapeadorMediaPrecoPorUnidadeAnoCategoria extends Mapper<Object, Text, Text, DoubleWritable> {
        private DoubleWritable preco = new DoubleWritable();
        private Text chaveDados = new Text();

        public void map(Object chave, Text texto, Context contexto) throws IOException, InterruptedException {
            String[] campos = texto.toString().split(",");
            String paisTransacao = campos[0];
            String anoTransacao = campos[1];
            String tipoFluxo = campos[4];
            String unidadeTransacao = campos[7];
            String categoriaTransacao = campos[9];

            if (paisTransacao.equals("Brasil") && tipoFluxo.equals("Export")) {
                double precoTransacao = Double.parseDouble(campos[5]);
                chaveDados.set(unidadeTransacao + " " + anoTransacao + " " + categoriaTransacao);
                preco.set(precoTransacao);
                contexto.write(chaveDados, preco);
            }
        }
    }

    public static class RedutorMediaPrecoPorUnidadeAnoCategoria extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable resultado = new DoubleWritable();

        public void reduce(Text chave, Iterable<DoubleWritable> valores, Context contexto) throws IOException, InterruptedException {
            double soma = 0.0;
            int contagem = 0;
            for (DoubleWritable valor : valores) {
                soma += valor.get();
                contagem++;
            }
            if (contagem > 0) {
                double media = soma / contagem;
                resultado.set(media);
                contexto.write(chave, resultado);
            }
            try {
                FileSystem fs = FileSystem.get(contexto.getConfiguration());
                Path outFile = new Path("output5/resultados.txt");
                FSDataOutputStream out = fs.create(outFile);
                out.writeBytes(chave.toString() + "\t" + resultado.toString() + "\n");
                out.close();
            } catch (IOException e) {
             e.printStackTrace();
            }
        }
    }

    public static class MapeadorPrecoMaxMinMedio extends Mapper<Object, Text, Text, Text> {
        private Text chaveDados = new Text();
        private Text valorDados = new Text();

        public void map(Object chave, Text texto, Context contexto) throws IOException, InterruptedException {
            String[] campos = texto.toString().split(",");
            String unidadeTransacao = campos[7];
            String anoTransacao = campos[1];
            double precoTransacao = Double.parseDouble(campos[5]);

            chaveDados.set(unidadeTransacao + " " + anoTransacao);
            valorDados.set(precoTransacao + " 1");
            contexto.write(chaveDados, valorDados);
        }
    }

    public static class RedutorPrecoMaxMinMedio extends Reducer<Text, Text, Text, Text> {
        private Text resultado = new Text();

        public void reduce(Text chave, Iterable<Text> valores, Context contexto) throws IOException, InterruptedException {
            double precoMaximo = Double.MIN_VALUE;
            double precoMinimo = Double.MAX_VALUE;
            double somaPrecos = 0.0;
            int contagem = 0;

            for (Text valor : valores) {
                String[] partes = valor.toString().split(" ");
                double preco = Double.parseDouble(partes[0]);
                int cont = Integer.parseInt(partes[1]);

                precoMaximo = Math.max(precoMaximo, preco);
                precoMinimo = Math.min(precoMinimo, preco);
                somaPrecos += preco;
                contagem += cont;
            }

            if (contagem > 0) {
                double precoMedio = somaPrecos / contagem;
                resultado.set("Máximo: " + precoMaximo + " Mínimo: " + precoMinimo + " Médio: " + precoMedio);
                contexto.write(chave, resultado);
            }

            try {
                FileSystem fs = FileSystem.get(contexto.getConfiguration());
                Path outFile = new Path("output6/resultados.txt");
                FSDataOutputStream out = fs.create(outFile);
                out.writeBytes(chave.toString() + "\t" + resultado.toString() + "\n");
                out.close();
            } catch (IOException e) {
             e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Job 1: Contar transações relacionadas ao Brasil
        Job job1 = Job.getInstance(conf, "MapReduce - Tarefa 1");
        job1.setJarByClass(MapReduce.class);
        job1.setMapperClass(MapeadorTransacoesBrasil.class);
        job1.setReducerClass(RedutorTransacoesBrasil.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path("input"));
        FileOutputFormat.setOutputPath(job1, new Path("output1"));
        job1.waitForCompletion(true);

        // Job 2: Contar transações por ano
        Job job2 = Job.getInstance(conf, "MapReduce - Tarefa 2");
        job2.setJarByClass(MapReduce.class);
        job2.setMapperClass(MapeadorTransacoesPorAno.class);
        job2.setReducerClass(RedutorTransacoesPorAno.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path("input"));
        FileOutputFormat.setOutputPath(job2, new Path("output2"));
        job2.waitForCompletion(true);

        // Job 3: Contar transações por tipo e ano
        Job job3 = Job.getInstance(conf, "MapReduce - Tarefa 3");
        job3.setJarByClass(MapReduce.class);
        job3.setMapperClass(MapeadorTransacoesTipoEAno.class);
        job3.setReducerClass(RedutorTransacoesTipoEAno.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job3, new Path("input"));
        FileOutputFormat.setOutputPath(job3, new Path("output3"));
        job3.waitForCompletion(true);

        // Job 4: Calcular a média de valores por ano
        Job job4 = Job.getInstance(conf, "MapReduce - Tarefa 4");
        job4.setJarByClass(MapReduce.class);
        job4.setMapperClass(MapeadorMediaValoresPorAno.class);
        job4.setReducerClass(RedutorMediaValoresPorAno.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job4, new Path("input"));
        FileOutputFormat.setOutputPath(job4, new Path("output4"));
        job4.waitForCompletion(true);

        // Job 5: Calcular a média de preços por unidade, ano e categoria
        Job job5 = Job.getInstance(conf, "MapReduce - Tarefa 5");
        job5.setJarByClass(MapReduce.class);
        job5.setMapperClass(MapeadorMediaPrecoPorUnidadeAnoCategoria.class);
        job5.setReducerClass(RedutorMediaPrecoPorUnidadeAnoCategoria.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job5, new Path("input"));
        FileOutputFormat.setOutputPath(job5, new Path("output5"));
        job5.waitForCompletion(true);

        // Job 6: Calcular valores máximo, mínimo e médio de preços
        Job job6 = Job.getInstance(conf, "MapReduce - Tarefa 6");
        job6.setJarByClass(MapReduce.class);
        job6.setMapperClass(MapeadorPrecoMaxMinMedio.class);
        job6.setReducerClass(RedutorPrecoMaxMinMedio.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job6, new Path("input"));
        FileOutputFormat.setOutputPath(job6, new Path("output6"));
        job6.waitForCompletion(true);
    }
}
