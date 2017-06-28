import java.awt.Color;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.util.ArrayList;
import java.util.StringTokenizer;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.border.EmptyBorder;
import javax.swing.border.LineBorder;

public class GUI extends JFrame {
  private static final long serialVersionUID = 1L;
  private JCheckBox[] param = new JCheckBox[21];
  private ArrayList<Integer> years;
  private String[] months =
      { "All", "January", "February", "March", "April", "May", "June", "July",
          "August", "September", "October", "November", "December" };
  private JComboBox<Object> yearI;
  private JComboBox<Object> yearII;
  private JComboBox<String> month;
  private int y1;
  private int y2;
  private ArrayList<Integer> pars;
  private JPanel paneLoad;
  private JPanel contentPane;
  private String[][] data = new String[1][1];
  private ArrayList<String> cols = new ArrayList<String>();
  private JScrollPane tab;
  private JTable table;

  public GUI(String input, String output) {
    setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    setBounds(100, 100, 600, 410);
    setResizable(false);

    contentPane = new JPanel();
    contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
    setContentPane(contentPane);
    contentPane.setLayout(null);

    paneLoad = new JPanel();
    paneLoad.setBorder(new EmptyBorder(5, 5, 5, 5));
    paneLoad.setLayout(null);

    JLabel load = new JLabel("Loading...");
    load.setFont(new Font("Tahoma", Font.BOLD, 22));
    load.setBounds(240, 50, 150, 50); // setBounds(x, y, largura, altura)
    paneLoad.add(load);

    paneLoad.setVisible(false);

    fillTableResult(output);

    JLabel parame = new JLabel("Parameters:");
    parame.setFont(new Font("Tahoma", Font.BOLD, 14));
    parame.setBounds(15, 5, 100, 18); // setBounds(x, y, largura, altura)
    contentPane.add(parame);

    parametres(contentPane);

    years = new ArrayList<Integer>();

    for (int i = 1929; i < 1931; i++) {
      years.add(i);
    }

    JLabel period = new JLabel("Period:");
    period.setFont(new Font("Tahoma", Font.BOLD, 14));
    period.setBounds(215, 5, 100, 18); // setBounds(x, y, largura, altura)
    contentPane.add(period);

    yearI = new JComboBox<Object>(years.toArray());
    yearI.setFont(new Font("Tahoma", Font.PLAIN, 10));
    yearI.setBounds(170, 30, 60, 30);
    contentPane.add(yearI);

    yearII = new JComboBox<Object>(years.toArray());
    yearII.setFont(new Font("Tahoma", Font.PLAIN, 10));
    yearII.setBounds(250, 30, 60, 30);
    contentPane.add(yearII);

    JLabel mon = new JLabel("Month:");
    mon.setFont(new Font("Tahoma", Font.BOLD, 14));
    mon.setBounds(369, 5, 100, 18); // setBounds(x, y, largura, altura)
    contentPane.add(mon);

    month = new JComboBox<String>(months);
    month.setFont(new Font("Tahoma", Font.PLAIN, 10));
    month.setBounds(350, 30, 90, 30);
    contentPane.add(month);

    table = new JTable(data, cols.toArray()) {
      private static final long serialVersionUID = 1L;

      public boolean isCellEditable(int row, int col) {
        return false;
      }
    };
    table.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
    table.setFont(new Font("Tahoma", Font.PLAIN, 16));
    table.setBorder(new LineBorder(Color.LIGHT_GRAY));
    table.setBackground(Color.WHITE);
    tab = new JScrollPane(table, JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED,
        JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
    tab.setBounds(160, 70, 430, 280);
    contentPane.add(tab);

    JButton btnMean = new JButton("Mean");
    btnMean.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent arg0) {
        executeMean(input, output);
      }
    });

    btnMean.setFont(new Font("Tahoma", Font.PLAIN, 10));
    btnMean.setBounds(470, 365, 120, 30);
    contentPane.add(btnMean);
  }

  public void parametres(JPanel pane) {
    param[3] = new JCheckBox("Temperature");
    param[3].setFont(new Font("Tahoma", Font.PLAIN, 10));
    param[3].setBounds(10, 30, 120, 30);

    param[5] = new JCheckBox("Dew Point");
    param[5].setFont(new Font("Tahoma", Font.PLAIN, 10));
    param[5].setBounds(10, 60, 120, 30);

    param[7] = new JCheckBox("Sea Level Pressure");
    param[7].setFont(new Font("Tahoma", Font.PLAIN, 10));
    param[7].setBounds(10, 90, 120, 30);

    param[9] = new JCheckBox("Station Pressure");
    param[9].setFont(new Font("Tahoma", Font.PLAIN, 10));
    param[9].setBounds(10, 120, 120, 30);

    param[11] = new JCheckBox("Visibility");
    param[11].setFont(new Font("Tahoma", Font.PLAIN, 10));
    param[11].setBounds(10, 150, 120, 30);

    param[13] = new JCheckBox("Wind Speed");
    param[13].setFont(new Font("Tahoma", Font.PLAIN, 10));
    param[13].setBounds(10, 180, 120, 30);

    param[15] = new JCheckBox("Maximum Wind Speed");
    param[15].setFont(new Font("Tahoma", Font.PLAIN, 10));
    param[15].setBounds(10, 210, 150, 30);

    param[16] = new JCheckBox("Maximum Wind Gust");
    param[16].setFont(new Font("Tahoma", Font.PLAIN, 10));
    param[16].setBounds(10, 240, 150, 30);

    param[17] = new JCheckBox("Maximum Temperature");
    param[17].setFont(new Font("Tahoma", Font.PLAIN, 10));
    param[17].setBounds(10, 270, 150, 30);

    param[18] = new JCheckBox("Minimum Temperature");
    param[18].setFont(new Font("Tahoma", Font.PLAIN, 10));
    param[18].setBounds(10, 300, 150, 30);

    param[19] = new JCheckBox("Total Precipitation");
    param[19].setFont(new Font("Tahoma", Font.PLAIN, 10));
    param[19].setBounds(10, 330, 120, 30);

    param[20] = new JCheckBox("Snow Depth");
    param[20].setFont(new Font("Tahoma", Font.PLAIN, 10));
    param[20].setBounds(10, 360, 120, 30);

    pane.add(param[3]);
    pane.add(param[5]);
    pane.add(param[7]);
    pane.add(param[9]);
    pane.add(param[11]);
    pane.add(param[13]);
    pane.add(param[15]);
    pane.add(param[16]);
    pane.add(param[17]);
    pane.add(param[18]);
    pane.add(param[19]);
    pane.add(param[20]);
  }

  public void executeMean(String input, String output) {
    if (!verifData())
      return;
    showLoad();
    JOptionPane.showMessageDialog(null, "Start processing");
    try {
      if (!Hadoop.executeMeanYear(y1, y2, input, output, pars)) {
        String[] buttons = { "Yes", "No" };
        int result = JOptionPane.showOptionDialog(null,
            "The output directory already exists. Would you like to erase it?",
            "Error", JOptionPane.WARNING_MESSAGE, 0, null, buttons, buttons[1]);
        if (result == 0) {
          Hadoop.deleteDir(new File(output));
          executeMean(input, output);
          return;
        }
      }
    } catch (Exception e) {
      JOptionPane.showMessageDialog(null, "Error during processing");
      e.printStackTrace();
    }
    GUI frame = new GUI(input, output);
    frame.setVisible(true);
    dispose();
  }

  private boolean verifData() {
    y1 = (Integer) yearI.getSelectedItem();
    y2 = (Integer) yearII.getSelectedItem();

    if (y1 > y2) {
      JOptionPane.showMessageDialog(null, "Period incorrect");
      return false;
    }

    pars = new ArrayList<Integer>();
    for (int i = 0; i < param.length; i++) {
      if (param[i] != null && param[i].isSelected()) {
        pars.add(i);
      }
    }

    if (pars.isEmpty()) {
      JOptionPane.showMessageDialog(null, "Select one parameter");
      return false;
    }

    return true;
  }

  public void showLoad() {
    contentPane.setVisible(false);
    paneLoad.setVisible(true);
    setContentPane(paneLoad);
  }

  private void fillTableResult(String output) {
    if (!new File(output + "/part-r-00000").exists()) {
      return;
    }
    TextReader read = new TextReader(output + "/part-r-00000");
    String lres = read.readLine(1);
    String aux = lres;
    StringTokenizer d;
    while (lres != null) {
      d = new StringTokenizer(lres);
      if (d.hasMoreTokens()) {
        cols.add(d.nextToken());
      }
      lres = read.readLine(1);
    }
    read.closeReader();
    
    read.openNewFile(output + "/part-r-00000");
    if (aux == null)
      return;
    d = new StringTokenizer(aux);
    data = new String[d.countTokens() - 1][cols.size()];
    for (int i = 0; i < data[0].length; i++) {
      int j = 0;
      d = new StringTokenizer(read.readLine(1));
      while (j < data.length) {
        try {
          String n = d.nextToken();
          n = String.format("%.3f", Float.parseFloat(n));
          data[j][i] = n;
          j++;
        } catch (NumberFormatException e) {
          continue;
        }
      }
    }
  }
}
