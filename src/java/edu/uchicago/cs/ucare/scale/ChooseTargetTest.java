package edu.uchicago.cs.ucare.scale;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.DatanodeDescriptor;
import org.apache.hadoop.dfs.DatanodeID;
import org.apache.hadoop.dfs.FSConstants;
import org.apache.hadoop.dfs.FSNamesystem;
import org.apache.hadoop.dfs.NameNode;
import org.apache.hadoop.net.NetworkTopology;

public class ChooseTargetTest {

    public static final int BLOCK_SIZE = 1024;
    public static final Configuration CONF = new Configuration();
    public static NetworkTopology cluster;
    public static NameNode namenode;
    public static FSNamesystem.ReplicationTargetChooser replicator;
    public static DatanodeDescriptor dataNodes[];

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Please enter number of datanodes");
            System.exit(0);
        }
        int numDatanodes = Integer.parseInt(args[0]);
        try {
            CONF.set("fs.default.name", "localhost:8020");
            NameNode.format(CONF);
            namenode = new NameNode(CONF);
        } catch (IOException e) {
            e.printStackTrace();
        }
        FSNamesystem fsNamesystem = FSNamesystem.getFSNamesystem();
        replicator = fsNamesystem.replicator;
        cluster = fsNamesystem.clusterMap;
        dataNodes = new DatanodeDescriptor[numDatanodes];
        for (int i = 0; i < numDatanodes; i++) {
            String hostname = "h" + i;
            String networkLoc = "/defaultrack";
            DatanodeDescriptor datanode = new DatanodeDescriptor(new DatanodeID(hostname + ":5020", "0", -1), networkLoc);
            cluster.add(datanode);
            dataNodes[i] = datanode;
        }
        for (int i = 0; i < numDatanodes; i++) {
            dataNodes[i].updateHeartbeat(2 * FSConstants.MIN_BLOCKS_FOR_WRITE
                    * BLOCK_SIZE, 2 * FSConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE, 0);
        }
        DatanodeDescriptor[] targets;
        long start = System.currentTimeMillis();
        for (int i = 0; i < 3; i++) {
            targets = replicator.chooseTarget(numDatanodes, dataNodes[0], null, BLOCK_SIZE);
        }
        long elapse = System.currentTimeMillis() - start;
        System.out.println("chooseTarget average elapse time = " + elapse / 3);
    }

}
