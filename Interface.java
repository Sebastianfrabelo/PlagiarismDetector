//-----------------------------------------------------------------//
// SCC0904 - Sistemas Computacionais Distribuidos - Trabalho Final //
//-----------------------------------------------------------------//
// Antonio Sebastian - NUSP 10797781                               //
// Joao Marcos Della Torre Divino - NUSP 10377708                  //
// Paulo Inay Cruz - NUSP 10388691                                 //
//-----------------------------------------------------------------//

// Bibliotecas utilizadas
import java.awt.event.*;
import java.awt.Color;
import java.awt.Font;
import java.awt.CardLayout;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import javax.swing.*;
import org.apache.hadoop.fs.Path;

//-----------------------------------------------------------------//
//                          Classe Interface                       //
//-----------------------------------------------------------------//
public class Interface implements ActionListener {

    // Variaveis relativas a janela
    JFrame frame;                       // Variavel para a janela
    ImageIcon icone;                    // Variavel para o icone da janela

    // Variaveis relativas aos paineis
    JPanel panelZero;                   // Variavel do painel base
    CardLayout cl;                      // Variavel para setar o layout do painel base
    JPanel panelOne;                    // Variavel da tela inicial
    JPanel panelTwo;                    // Variavel da tela final

    // Variaveis relativas ao panelOne
    JLabel label;                       // Variavel do label para o logo da aplicacao
    ImageIcon logo;                     // Variavel para a imagem do logo
    JButton fileButton;                 // Variavel para o botao de selecao do arquivo de entrada
    JLabel fileName;                    // Variavel do label para mostrar o nome do arquivo de entrada selecionado
    JButton processButton;              // Variavel para o botao de inicializacao do processamento

    // Variaveis relativas ao panelTwo
    JTextArea myText;                   // Variavel para receber o texto do arquivo de entrada
    JLabel myTextLabel;                 // Variavel para mostrar o nome do arquivo de entrada sobre sua caixa de texto
    JTextArea plagioText;               // Variavel para receber o texto de um arquivo plagiado
    JLabel plagioLabel;                 // Variavel para mostrar quantos arquivos foram plagiados
    JLabel plagioPercent;               // Variavel para mostrar o grau de plagio relativo a um arquivo
    JComboBox<String> filesPlagio;      // Variavel para selecionar um dos arquivos plagiados
    JScrollPane scrollPaneOne;          // Variavel para criar um panel com scroll
    JScrollPane scrollPaneTwo;          // Variavel para criar um panel com scroll

    // Variaveis relativas ao arquivo de entrada
    JFileChooser fileChooser;
    String fileContent;
    
    // Variavel relativa aos arquivos plagiados
    String[] infoPlagios;
    

    //-----------------------------------------------------------------//
    //                        Construtor da classe                     //
    //-----------------------------------------------------------------//
    Interface(){

        //-----------------------------------------------------------------//
        //          Inicializacao do painel base e de seu layout           //
        //-----------------------------------------------------------------//
        panelZero = new JPanel();
        cl = new CardLayout();

        //-----------------------------------------------------------------//
        //              Componentes do panelOne - Tela Inicial             //
        //-----------------------------------------------------------------//
        // Label para o logo da aplicacao
        label = new JLabel();
        // Setar o path do logo
        logo = new ImageIcon("logo.png");
        label.setIcon(logo);
        label.setBounds(150, 150, 500, 120);

        // Botão para selecao do arquivo de entrada
        fileButton = new JButton();
        fileButton.addActionListener(this);
        fileButton.setText("Selecione um arquivo");
        fileButton.setFocusable(false);
        fileButton.setBounds(300, 300, 200, 25);

        // Label para mostrar o nome do arquivo selecionado
        fileName = new JLabel();
        fileName.setBounds(150, 350, 500, 25);
        fileName.setVisible(false);

        // Botão para iniciar o processamento para detectar plagio
        processButton = new JButton();
        processButton.setText("Iniciar processamento");
        processButton.setFocusable(false);
        processButton.setBounds(300, 400, 200, 25);
        processButton.setEnabled(false);

        //-----------------------------------------------------------------//
        //             Configuracao do panelOne - Tela Inicial             //
        //-----------------------------------------------------------------//
        panelOne = new JPanel();
        panelOne.setBackground(Color.white);
        panelOne.setLayout(null);
        panelOne.add(label);
        panelOne.add(fileButton);
        panelOne.add(fileName);
        panelOne.add(processButton);

        //-----------------------------------------------------------------//
        //               Componentes do panelTwo - Tela Final              //
        //-----------------------------------------------------------------//
        // TextArea para imprimir o texto do arquivo de entrada selecionado
        myText = new JTextArea();
        myText.setLineWrap(true);
        myText.setWrapStyleWord(true);
        myText.setEditable(false);

        // Label para mostrar o nome do arquivo de entrada selecionado
        myTextLabel = new JLabel();
        myTextLabel.setBounds(25, 50, 350, 50);
        myTextLabel.setFont(new Font("Arial",Font.BOLD,16));

        // TextArea para imprimir o texto de um arquivo que foi plagiado
        plagioText = new JTextArea();
        plagioText.setLineWrap(true);
        plagioText.setWrapStyleWord(true);
        plagioText.setEditable(false);
        
        // Label para mostrar a quantidade de textos plagiados
        plagioLabel = new JLabel("Correspondências para plágio: ", SwingConstants.CENTER);
        plagioLabel.setBounds(400, 25, 350, 50);
        plagioLabel.setFont(new Font("Arial",Font.BOLD,16));

        // Label para mostrar o grau de plagio com relacao a um determinado arquivo
        plagioPercent = new JLabel("Grau de plágio: ", SwingConstants.CENTER);
        plagioPercent.setBounds(400, 500, 350, 50);
        plagioPercent.setFont(new Font("Arial",Font.BOLD,16));

        // ComboBox para selecionar um dos arquivos plagiados para que seu conteudo seja mostrado
        filesPlagio = new JComboBox<String>();
        filesPlagio.addItem("Selecione um arquivo...");
        filesPlagio.addActionListener(this);
        filesPlagio.setBounds(400, 75, 350, 25);

        // ScrollPane para definir uma caixa de texto com scrollbar
        scrollPaneOne = new JScrollPane(myText);
        scrollPaneOne.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED);
        scrollPaneOne.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_NEVER);
        scrollPaneOne.setBounds(25, 125, 350, 375);
        scrollPaneTwo = new JScrollPane(plagioText);
        scrollPaneTwo.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED);
        scrollPaneTwo.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_NEVER);
        scrollPaneTwo.setBounds(400, 125, 350, 375);


        //-----------------------------------------------------------------//
        //              Configuracao do panelTwo - Tela Final              //
        //-----------------------------------------------------------------//
        panelTwo = new JPanel();
        panelTwo.setBackground(Color.lightGray); // Muda a cor do backgroun
        panelTwo.setLayout(null);
        panelTwo.add(myTextLabel);
        panelTwo.add(scrollPaneOne);
        panelTwo.add(plagioLabel);
        panelTwo.add(scrollPaneTwo);
        panelTwo.add(plagioPercent);
        panelTwo.add(filesPlagio);

        //-----------------------------------------------------------------//
        //              Configuracao do panelZero - Tela Base              //
        //-----------------------------------------------------------------//
        panelZero.setLayout(cl);
        panelZero.add(panelOne, "telaInicial");
        panelZero.add(panelTwo, "telaFinal");
        cl.show(panelZero, "telaInicial");

        //-----------------------------------------------------------------//
        //                  Funcao para o botão processButton              //
        //-----------------------------------------------------------------//
        //      Esta funcao permite que, clicado o botao, seja iniciado o  //
        // processamento do arquivo de entrada de maneira a buscar por     //
        // ocorrencias de plagio. Uma vez clicado, nao eh possivel seleci- //
        // ona-lo novamente, nem mudar o arquivo selecionado. Ao final do  //
        // processamento, ocorre a mudanca para a tela final da aplicacao, //
        // onde o usuario pode comparar seu texto com aqueles que tiveram  //
        // correspondencia.                                                //
        //-----------------------------------------------------------------//
        processButton.addActionListener(new ActionListener(){

            @Override
            public void actionPerformed(ActionEvent e) {
                
                fileButton.setEnabled(false);
                processButton.setEnabled(false);

                // Chamar o Hadoop
                MapReduceInterface mapReduceInterface = new MapReduceInterface(fileContent);
                String arquivos = new String();
                Path caminho;
                try{
                            mapReduceInterface.startJob();
                    caminho = new Path("/files/output/");
                    arquivos = HDFSInterface.getOutput(caminho);
                } catch (Exception error){
                    error.printStackTrace();
                }
                
                cl.show(panelZero, "telaFinal");
                myText.setText(fileContent);
                myTextLabel.setText("Arquivo de entrada: " + fileChooser.getSelectedFile().getName());
                myTextLabel.setHorizontalAlignment(SwingConstants.CENTER);

                // Inserir os arquivos de saida no ComboBox
                List<String> textNamePlagios = new ArrayList<String>();
                int i = 0;

                infoPlagios = arquivos.split("\\s|\n");
                for (int x=0; x < infoPlagios.length; x++){
                    if(infoPlagios[x].contains(".txt")){
                        textNamePlagios.add(infoPlagios[x]);
                        filesPlagio.addItem(textNamePlagios.get(i));
                        i++;
                    }
                }

                // Atualiza o label de correspondência de plagio
                plagioLabel.setText(plagioLabel.getText() + textNamePlagios.size());

            }

        }); // Fim da funcao do botao processButton

        //-----------------------------------------------------------------//
        //              Configuracao da janela da aplicacao                //
        //-----------------------------------------------------------------//
        frame = new JFrame("Detector de Plágio");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(800, 600);
        frame.setResizable(false);
        frame.add(panelZero);
        icone = new ImageIcon("icone.png");
        frame.setIconImage(icone.getImage());
        frame.getContentPane().setBackground(Color.lightGray);
        frame.setVisible(true);

    } // Fim do construtor da classe


    //-----------------------------------------------------------------//
    //         Funcao para o botão fileButton e para a ComboBox        //
    //-----------------------------------------------------------------//
    @Override
    public void actionPerformed(ActionEvent e) {
        
        // Se o botao de selecao do arquivo de entrada for clicado
        if (e.getSource().equals(fileButton)) {

            // Abre a janela para selecao de arquivos
            fileChooser = new JFileChooser();
            int response = fileChooser.showOpenDialog(null);

            // Caso algum arquivo seja selecionado, ele eh aberto e seu conteudo
            // salvo na string fileContent
            if (response == JFileChooser.APPROVE_OPTION) {
                File file = new File(fileChooser.getSelectedFile().getAbsolutePath());

                // Configura o label fileName
                fileName.setText("Arquivo selecionado: " + fileChooser.getSelectedFile().getName());
                fileName.setHorizontalAlignment(SwingConstants.CENTER);
                fileName.setVisible(true);

                try {

                    // Le o arquivo de entrada selecionado
                    Scanner scan = new Scanner(file);
                    fileContent = "";

                    while (scan.hasNextLine()) {
                        fileContent = fileContent.concat(scan.nextLine() + "\n");
                    }

                    scan.close();

                    // Habilita o botao de processamento
                    processButton.setEnabled(true);


                } catch (FileNotFoundException e1) {
                    e1.printStackTrace();
                }

            }

        } // Fim do if para o fileButton

        if(e.getSource().equals(filesPlagio)){
            
            // Ler o arquivo de texto selecionado e printar na TextArea
            String pathInputString = "/files/input/" + filesPlagio.getSelectedItem();
            Path pathInput = new Path(pathInputString);
            try{
                plagioText.setText(HDFSInterface.getFile(pathInput));
            } catch (IOException e2) {
                e2.printStackTrace();
            }
            
            // Atualizar o label de porcentagem de plagio
            for(int i = 0; i < infoPlagios.length; i++){
                if(filesPlagio.getSelectedItem().equals(infoPlagios[i])){
                    plagioPercent.setText("Grau de plágio: " + infoPlagios[i+1]);
                    break;
                }
            }

        }

    }

    //-----------------------------------------------------------------//
    //                              Main                               //
    //-----------------------------------------------------------------//
    public static void main(String[] args){
        
        new Interface();

    }// Fim da main

} // Fim da classe Interface
