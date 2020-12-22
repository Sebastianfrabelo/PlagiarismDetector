/***********************************************************************


    Classe HDFSInterface

      Contem metodos que facilitam a comunicacao com o HDFS


***********************************************************************/

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;

// Declaracao da classe
public class HDFSInterface {

    /******************* Metodo getOutput() *******************/
    // Parametro:
    //  Path outputPath: caminho no HDFS aonde se encontram os arquivos de saida produzidos pelo Hadoop.
    // Saida:
    //  String outputString: string que contem a saida completa gerada pelo Hadoop.
    // Funcionamento:
    //  Retornara uma string que contem os dados de todos os arquivos de saida gerados pelo Hadoop concatenados.
    //  Caso nao existam arquivos de saida, retorna uma string vazia.
    public static String getOutput(Path outputPath) throws IOException{

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        FSDataInputStream fsIn = null;
        String outputPathString = outputPath.toString(); // String do caminho utilizado
        String fileNumber = "";
        String fileOutputName = "";
        String outputString = "";

        for(int i = 0; i <= 99999; i++){ // 99999 eh o numero maximo de saidas impressas pelo Hadoop
            fileNumber = String.format("%d", i); // As linhas seguintes decidem o nome e caminho do arquivo. 
                                                 // Todos, neste projeto, terao o formato: "/files/output/part-r-00000"
                                                 // onde 00000 pode ser trocado por outro numero.
            if(fileNumber.length() < 5){
                int k = fileNumber.length();
                for (int j = 0; j < 5 - k; j++) fileNumber = "0" + fileNumber;
            }
            fileOutputName = outputPathString + "/part-r-" + fileNumber;
            Path pathIn = new Path(fileOutputName);

            if(fs.isFile(pathIn)){ // Verifica se existe um arquivo com o nome criado. Se sim, a string concatena este arquivo.
                                   // Se nao o loop sera quebrado.
                fsIn = fs.open(pathIn);
                outputString = outputString + IOUtils.toString(fsIn, StandardCharsets.UTF_8.name());
            } else break;
        }

        return outputString;
    }

    /******************* Metodo getFile() *******************/
    // Parametro:
    //  Path filePath: caminho no HDFS aonde se encontram o arquivo que o usuario deseja abrir.
    // Saida:
    //  String outputString: string que contem todo o conteudo do arquivo.
    // Funcionamento:
    //  Retorna uma string com o conteudo do arquivo escolhido caso ele exista. Se nao retorna uma string vazia.
    public static String getFile(Path filePath) throws IOException{

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        FSDataInputStream fsIn = null;
        String outputString = "";

        if(fs.isFile(filePath)){
            fsIn = fs.open(filePath);
            outputString = IOUtils.toString(fsIn, StandardCharsets.UTF_8.name());
        }

        return outputString;
    }
}