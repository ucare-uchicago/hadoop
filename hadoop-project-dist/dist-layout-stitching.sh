run() {
                        echo "\$ ${@}"
                        "${@}"
                        res=$?
                        if [ $res != 0 ]; then
                          echo
                          echo "Failed!"
                          echo
                          exit $res
                        fi
                      }

                      ROOT=`cd ../..;pwd`
                      echo
                      echo "Current directory `pwd`"
                      echo
                      run rm -rf hadoop-0.24.0-SNAPSHOT
                      run mkdir hadoop-0.24.0-SNAPSHOT
                      run cd hadoop-0.24.0-SNAPSHOT
                      #run cp $ROOT/LICENSE.txt .
                      #run cp $ROOT/NOTICE.txt .
                      #run cp $ROOT/README.txt .
                      run cp -r $ROOT/hadoop-common-project/hadoop-common/target/hadoop-common-0.24.0-SNAPSHOT/* .
                      #run cp -r $ROOT/hadoop-common-project/hadoop-nfs/target/hadoop-nfs-0.24.0-SNAPSHOT/* .
                      run cp -r $ROOT/hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-0.24.0-SNAPSHOT/* .
                      #run cp -r $ROOT/hadoop-hdfs-project/hadoop-hdfs-httpfs/target/hadoop-hdfs-httpfs-0.24.0-SNAPSHOT/* .
                      #run cp -r $ROOT/hadoop-common-project/hadoop-kms/target/hadoop-kms-0.24.0-SNAPSHOT/* .
                      #run cp -r $ROOT/hadoop-hdfs-project/hadoop-hdfs-nfs/target/hadoop-hdfs-nfs-0.24.0-SNAPSHOT/* .
                      #run cp -r $ROOT/hadoop-yarn-project/target/hadoop-yarn-project-0.24.0-SNAPSHOT/* .
                      #run cp -r $ROOT/hadoop-mapreduce-project/target/hadoop-mapreduce-0.24.0-SNAPSHOT/* .
run mkdir -p share/hadoop/mapreduce
run cp -r $ROOT/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/target/hadoop-mapreduce-client-app-0.24.0-SNAPSHOT* share/hadoop/mapreduce/
run cp -r $ROOT/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-common/target/hadoop-mapreduce-client-common-0.24.0-SNAPSHOT* share/hadoop/mapreduce/
run cp -r $ROOT/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/target/hadoop-mapreduce-client-core-0.24.0-SNAPSHOT* share/hadoop/mapreduce/
run cp -r $ROOT/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/target/hadoop-mapreduce-client-hs-0.24.0-SNAPSHOT* share/hadoop/mapreduce/
run cp -r $ROOT/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/target/hadoop-mapreduce-client-jobclient-0.24.0-SNAPSHOT* share/hadoop/mapreduce/
run cp -r $ROOT/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-shuffle/target/hadoop-mapreduce-client-shuffle-0.24.0-SNAPSHOT* share/hadoop/mapreduce/
run mkdir -p share/hadoop/assemblies
run cp -r $ROOT/hadoop-assemblies/target/hadoop-assemblies-0.24.0-SNAPSHOT.jar share/hadoop/assemblies/
                      #run cp -r $ROOT/hadoop-tools/hadoop-tools-dist/target/hadoop-tools-dist-0.24.0-SNAPSHOT/* .
                      echo
                      echo "Hadoop dist layout available at: /mnt/extra/hadoop/hadoop-project-dist/target/hadoop-0.24.0-SNAPSHOT"
                      echo
