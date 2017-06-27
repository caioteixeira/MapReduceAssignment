import java.awt.EventQueue;

public class Index {
  public static void main(String[] args) throws Exception {
    EventQueue.invokeLater(new Runnable() {
      public void run() {
        try {
          GUI frame = new GUI(args[0], args[1]);
          frame.setVisible(true);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
  }
}
