/***********************************************************************


    Classe MapReduceInterface

      Contem metodos especificos ao processo do MapReduce, em outras
      palavras o job enviado para ser executado pelo Hadoop.


***********************************************************************/

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Collections;
import java.util.StringTokenizer;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;


// Declaracao da classe
public class MapReduceInterface {

  private String input; // Essa variavel contem o texto que sera comparado com o banco de dados.

  MapReduceInterface(String input){
	this.input = input;
  }


  /******************* Metodo similarityDetector() *******************/
  // Parametros:
  //    String text1: texto de entrada, no ambito do projeto seria o texto a ser comparado com o banco de dados.
  //    String paragrafo2: paragrafo de texto a ser comparado com text1. Durante o procedimento de map o Hadoop ira
  //        emitir como valor a ser tratado na funcao apenas um paragrafo do arquivo mapeado.
  // Saida:
  //    Retorna um tipo String no formato "<grau de similaridade> <paragrafosemelhante1> <paragrafosemelhante2>"
  private static String similarityDetector(String text1, String paragrafo2){
    int aux = 0,i = 0,j = 0, menor = 0, maior = 0;

    String[] palavras2 = paragrafo2.split(" ");

    String[] paragrafos1 = text1.split("\n");
    String[][] palavras1;
    //cada palavra é uma string mapeada com espaço
    palavras1 = new String[paragrafos1.length][];

    for(i=0;i<paragrafos1.length;++i){
        palavras1[i] = paragrafos1[i].split(" ");
    }
    String similares;

    for(i=0;i<paragrafos1.length;++i){
        aux=0;
        menor = (palavras1[i].length < palavras2.length? palavras1[i].length : palavras2.length - 1);
        for(j=0; j < menor;++j){
            if(palavras1[i][j].equals(palavras2[j+1])){   //palavras2[j+1] o primeiro será o índice do paragrafo
                aux++;
            }else{
                aux = 0;    //avaliará nao só o início do paragrafo, podendo reiniciar a contagem de similaridade      
            }
            if(aux>maior)
                    maior = aux;
            
            if (j==(menor-1) && maior>5){   //verifica até o fim do paragrafo, pode ser que algum trecho no final seja ainda mais similar
                String Smaior,Sparagrafo,Scomparador;
                Smaior = Integer.toString(maior);
                Sparagrafo = Integer.toString(i+1);
                        
                similares = Smaior + " " + Sparagrafo + " " + palavras2[0];
                
                return(similares);
            }
        }
    }
    return "";
  }

  // Classe responsavel pelo metodo map utilizado pelo Hadoop.
  public static class SimilarityMapper
       extends Mapper<Object, Text, Text, Text>{
  
    private Text sendValue = new Text(); // Valor a ser emitido junto com a chave no metodo map().
    private String mapInput;

    protected void setup(Context context) throws IOException, InterruptedException{
	Configuration conf = context.getConfiguration();
	mapInput = conf.get("input.file.parameter");
    }
  
    /******************* Metodo map() *******************/
    // Parametros:
    //  Object key: chave de entrada da funcao (emitida pelo Hadoop). Nao eh utilizada.
    //  Text value: valor de entrada da funcao (emitido pelo Hadoop). Sera, neste caso, uma linha de algum arquivo que
    //      se encontrado no banco de dados.
    //  Context context: contexto do framework MapReduce. Nele sera emitido um par de valores (chave e valor).
    // Funcionamento:
    //  A funcao ira comparar o arquivo de entrada com o paragrafo do BD emitido pelo Hadoop e entao emitira
    //  o nome do arquivo como chave e a saida da funcao similarityDetector() como valor no contexto.
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
  
      String valueString = value.toString();

      System.out.println("valueString1: " + valueString);
      System.out.println("input: " + mapInput);

      valueString = similarityDetector(mapInput, valueString); // Verifica similaridade
      sendValue.set(valueString);

      System.out.println("valueString2: " + valueString);
  
      FileSplit fileSplit = (FileSplit)context.getInputSplit(); // Encontra o nome do arquivo
      String filename = fileSplit.getPath().getName();
      Text sendFilename = new Text();
      sendFilename.set(filename);

      context.write(sendFilename, sendValue); // Chave enviada: nome do arquivo
                                              // Valor enviado: valor de saida de similarityDetector()
    }
  }

  // Classe responsavel pelo metodo reduce utilizado pelo Hadoop.
  public static class SortReducer
       extends Reducer<Text,Text,Text,Text> {

    private Text result = new Text(); // Valor a ser emitido junto com a chave no metodo reduce().

    /******************* Metodo reduce() *******************/
    // Parametros:
    //  Text key: uma das chaves emitidas em map()
    //  Iterable<Text> values: contem todos os valores que foram emitidos com a chave key no contexto.
    //      Como essa funcao eh utilizada tanto para combine() quando para reduce(), esse contexto pode
    //      ser o contexto geral e tambem pode ser o contexto local.
    //  Context context: contexto do framework MapReduce. Nele sera emitido um par de valores (chave e valor).
    // Funcionamento:
    //  O objetivo desta funcao eh reduzir todos os dados emitidos por map() em uma unica linha para cada 
    //  arquivo de entrada. Dessa forma a funcao procurara entre os dados emitidos aquele que possui o maior
    //  grau de semelhanca e juntara todos os pares de paragrafos semelhantes na mesma linha.
    //  A chave enviada ainda eh o nome do arquivo, mas o valor sera uma string contendo o maior grau de
    //  similaridade encontrado nele e os paragrafos que foram considerados similares.
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      List<String> valueStrings = new ArrayList<String>();
      String resultString = new String();

      for (Text val : values){
        valueStrings.add(val.toString());
      }

      Collections.sort(valueStrings, Collections.reverseOrder());

      int count = 0;
  
      for(String valueString : valueStrings){
        StringTokenizer itr = new StringTokenizer(valueString);
	try {
        	if(count == 0) resultString = itr.nextToken();
        	else itr.nextToken();
        	while(itr.hasMoreTokens()) {
            		resultString = resultString.concat(" " + itr.nextToken());
        	}
	}
	catch (Exception e){
		continue;
	}
        count++;
      }

      if(!resultString.isEmpty()){
        result.set(resultString);
        context.write(key,result);
      }
    }
  }

  /******************* Metodo startJob() *******************/
  // Funcionamento:
  //    Responsavel por configurar e comecar o job MapReduce que sera passado para o Hadoop.
  public void startJob() throws Exception {
    Configuration conf = new Configuration();
    conf.set("input.file.parameter", this.input);

    Job job = Job.getInstance(conf, "plagiarism");
    job.setJarByClass(MapReduceInterface.class); // Classe principal relacionada ao MapReduce
    job.setMapperClass(SimilarityMapper.class); // Classe que contem o metodo map()
    job.setCombinerClass(SortReducer.class); // Classe que contem o metodo combine() (nesse caso sera o mesmo que reduce())
    job.setReducerClass(SortReducer.class); // Classe que contem o metodo reduce()
    job.setOutputKeyClass(Text.class); // Tipo de dado utilizado para a emissao de chaves
    job.setOutputValueClass(Text.class); // Tipo de dado utilizado para a emissao de valores

    FileInputFormat.addInputPath(job, new Path("/files/input/")); // Caminho que contem os arquivos de entrada (banco de dados)
    FileSystem fs = FileSystem.get(conf);
    Path out = new Path("/files/output/"); // Caminho aonde sera impresso o arquivo de saida do job
    fs.delete(out,true); // Deletar caminho de saida caso ele ja exista: evitar problemas na chamada do Hadoop
    FileOutputFormat.setOutputPath(job, out); // Settar caminho

    job.waitForCompletion(true); // Executa o job

    String outputString = HDFSInterface.getOutput(out);

    System.out.println(outputString);

  }
}
