package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.*;
/**
 * Created by huanke on 11/27/16.
 */
public class EventInterceptor {
    private static final Log LOG = LogFactory.getLog(EventInterceptor.class);
    int nodeId;
    int nodeType;

    int sendNode;
    int recvNode;
    int nodeState; //1 represents alived and o represents crashed.
    InterceptEventType interceptEventType;
    int interceptEventTypeId;
    int eventId;

    String fileDir;
    String filename;

    boolean samcResponse;

    public EventInterceptor(Role roleS, Role roleR, int nodeState, InterceptEventType interceptEventType){
        this.sendNode=getRoleId(roleS);
        this.recvNode=getRoleId(roleR);
        this.nodeState=nodeState;
        this.interceptEventType=interceptEventType;
        interceptEventTypeId=getTypeId(interceptEventType);
        eventId=getEventId();
        fileDir=getFileDir();
        filename=createFileName();
        samcResponse=false;

        //begin to write /new/filename
        try{
            String newFileName=fileDir+"/new/"+filename;
            PrintWriter writer = new PrintWriter(newFileName,"UTF-8");
            writer.print("sendNode="+this.sendNode+"\n");
            writer.print("recvNode="+this.recvNode+"\n");
            writer.print("nodeState="+this.nodeState+"\n");
            writer.print("interceptEventType="+interceptEventTypeId+ "\n");
            writer.print("type="+this.interceptEventType+ "\n");
            writer.print("eventId="+eventId);
            writer.close();
        }catch (IOException e){
            e.printStackTrace();
        }
        commitEvent();
        // waitAck();
    }


    public int getRoleId(Role role){
        switch (role){
            case RM: this.nodeId=1; break;
            case NM: this.nodeId=2; break;
            case AM: this.nodeId=3; break;
            default:this.nodeId=0;break;
        }
        return this.nodeId;
    }

    public int getTypeId(InterceptEventType interceptEventType){
        switch (interceptEventType){
            case NodeStatusUpdate: this.nodeType=1;break;
            default:this.nodeType=0;break;
        }
        return this.nodeType;
    }

    public void commitEvent(){
        try {
            String moveFile="mv "+fileDir+"/new/"+filename+" "+fileDir+"/send/"+filename;
            Process p = Runtime.getRuntime().exec(moveFile);
            Thread.sleep(200);
            LOG.info("@HK "+moveFile);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void waitAck(){
        String ackFileName=fileDir+"/ack/"+filename;
        FileReader fileReader= null;
        BufferedReader inRead=null;
        File ackFile=new File(ackFileName);

        //step1 wait for ackFile in /tmp/ipc/ack/ackFile
        while (!ackFile.exists()){
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        LOG.info("@HK Step1 -> wait for ack File "+ackFileName);

        //step2 read the execute=true or false in ackFile
        try {
            fileReader = new FileReader(ackFileName);
            inRead = new BufferedReader(fileReader);
            String line;
            try {
                while ((line = inRead.readLine())!=null){
                    LOG.info("@HK "+line);
                    if(line.equals("execute=true")){
                        samcResponse=true;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        LOG.info("@HK Step2 -> read ack File "+ackFileName);

        //step3 remove the /tmp/ipc/ack/ackFile
        try {
            Runtime.getRuntime().exec("rm -r "+fileDir+"/ack/"+filename);
        } catch (IOException e) {
            e.printStackTrace();
        }
        LOG.info("@HK Step3 -> remove ack File "+ackFileName);
    }


    public String getFileDir(){
        String fileDir="/tmp/ipc";
        return fileDir;
    }

    public String createFileName(){
        long time=System.currentTimeMillis();
        int count=(int) (time % 10000);
        String filename="hadoop-"+String.valueOf(eventId)+"-"+String.valueOf(count);
        LOG.info("@HK "+filename);
        return filename;
    }

    public int getEventId(){
        int prime = 19;
        int hash=1;
        hash = prime*hash+this.sendNode;
        hash = prime*hash+this.recvNode;
        hash = prime*hash+this.interceptEventTypeId;
        return hash;
    }

    public boolean getSAMCResponse(){
        return samcResponse;
    }

    public void printString(){
        LOG.info("@HK sendNode: "+this.sendNode+" recvNode: "+this.recvNode+" interceptEventType: "+this.interceptEventType.toString());
    }
}
