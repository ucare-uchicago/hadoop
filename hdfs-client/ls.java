import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class ls{
    public static void main (String [] args) throws Exception{
        try{
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS","hdfs://node-0:9000");
            FileSystem hdfs = FileSystem.get(new Configuration());
            FileStatus[] status = hdfs.listStatus(new Path("/"));  // you need to pass in your hdfs path

            // ls
	    /*FileStatus[] fsStatus = fs.listStatus(new Path("/"));
            for(int i = 0; i < fsStatus.length; i++){
                System.out.println(fsStatus[i].getPath().toString());
	    }*

            //cat
            /*for (int i=0;i<status.length;i++){
                BufferedReader br=new BufferedReader(new InputStreamReader(hdfs.open(status[i].getPath())));
                String line;
                line=br.readLine();
                while (line != null){
                    System.out.println(line);
                    line=br.readLine();
                }
            }*/

            // write file
            int nrFiles = Integer.parseInt(args[0]);
            int nrBlocksPerFile = Integer.parseInt(args[1]);
            byte[] randbyte = new byte[8*1024];
            Random rd = new Random();
            rd.nextBytes(randbyte);
            for (int i=0; i<nrFiles; i++) {
                String fileName = "test-"+i+".txt";
                Path file = new Path("/home/ubuntu/"+fileName);
                if ( hdfs.exists( file )) { hdfs.delete( file, true ); } 
                OutputStream os = hdfs.create( file,
                    new Progressable() {
                        public void progress() {
                            System.out.print(".");
                        } });
                System.out.print("Writing "+fileName);
                for (int j=0; j<nrBlocksPerFile; j++) {
                    os.write(randbyte);
                    os.flush();
                }
                os.close();
                System.out.println("");
            }
            hdfs.close();
        }catch(Exception e){
            System.out.println("File not found");
        }
    }
}
