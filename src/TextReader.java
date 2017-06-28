import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class TextReader {
  private FileReader file;
  private BufferedReader read;
	
  public TextReader(String addr) {
		try{
      this.file = new FileReader(new File(addr).getAbsolutePath());
      this.read = new BufferedReader(file);
		}catch(IOException e){
      System.out.println("Error reading file:" + e.getMessage());
		}
	}
	
  public String readLine(int lins) {
    String line = null;
		try{
      line = read.readLine();
      String lin_aux = line;
			int i = 1;
      while (lin_aux != null && i < lins) {
        lin_aux = read.readLine();
        line += ("\n" + lin_aux);
				i++;
			}
		} catch(IOException e){
      System.out.println("Error opening file:" + e.getMessage());
		}
    return line;
	}
	
  public String readAll() {
    String line = null;
		try{
      line = read.readLine();
      String lin_aux = line;
      while (lin_aux != null) {
        lin_aux = read.readLine();
        line += ("\n" + lin_aux);
			}
		} catch(IOException e){
      System.out.println("Error opening file:" + e.getMessage());
		}
    return line;
	}
	
  public void closeReader() {
		try{
      file.close();
		}catch(IOException e){
      System.out.println("Error closing file:" + e.getMessage());
		}
	}

  public void openNewFile(String addr) {
    try {
      this.file = new FileReader(new File(addr).getAbsolutePath());
      this.read = new BufferedReader(file);
    } catch (IOException e) {
      System.out.println("Error reading file:" + e.getMessage());
    }
  }
}
