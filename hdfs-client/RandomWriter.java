import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class RandomWriter {
  public static void main(String [] args) throws Exception {
    try{
      Configuration conf = new Configuration();
      conf.set("fs.default.name","hdfs://node-0");
      FileSystem hdfs = FileSystem.get(new Configuration());

      // write file
      int nrFiles = Integer.parseInt(args[0]);
      int nrBlocksPerFile = Integer.parseInt(args[1]);
      String filePrefix = args[2];
      byte[] randbyte = new byte[1024];

      Random rd = new Random();
      rd.nextBytes(randbyte);
      for (int i=0; i<nrFiles; i++) {
        String fileName = filePrefix+"-"+i+".txt";
        Path file = new Path("/home/ubuntu/"+fileName);
        if (hdfs.exists(file))
          hdfs.delete( file, true ); 
        OutputStream os = hdfs.create( file,
          new Progressable() {
            public void progress() {
              System.out.print(".");
            } });
        System.out.print("Writing "+fileName);
        for (int j=0; j<nrBlocksPerFile; j++) {
          os.write(randbyte);
        }
        os.flush();
        os.close();
        Thread.sleep(1000);
        System.out.println("");
      }
      hdfs.close();
    }catch(Exception e){
      System.out.println("File not found");
    }
  }
}
