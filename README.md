## Data Intensive Architecture Project

Datasets: 

1. U.S. Government’s Open Data Platform. "Crime Data from 2020 to Present." [https://catalog.data.gov/dataset/crime-data-from-2020-to-present](https://catalog.data.gov/dataset/crime-data-from-2020-to-present), accessed April 2024.
2. U.S. Government’s Open Data Platform. "Arrest Data from 2020 to Present." [https://catalog.data.gov/dataset/arrest-data-from-2020-to-present](https://catalog.data.gov/dataset/arrest-data-from-2020-to-present), accessed April 2024.

Installation Requirements:
       1. Hadoop 3.4.0
       2. Apache Spark
       3. JAVA , Open-JDK & default-JRE
       3. Python
       4. pip
       5. pip installpyspark
       6. pip install pandas
       7. pip install plotly
       8. pip installpyspark
       9. pip install matplotlib
       20. pip install jupyternote
 
Setup Instructions:
		  sudo apt update
		  sudo apt install default-jre
	 	  sudo apt install default-jdk
		  sudo vim /etc/sysctl.conf
          net.ipv6.conf.all.disable_ipv6=1
          net.ipv6.conf.default.disable_ipv6=1
          net.ipv6.conf.lo.disable_ipv6=1
          sudo sysctl -p
			sudo groupadd hadoop
			sudo useradd -ghadoop hduser -m -s /bin/bash
			sudo passwd hduser
			sudo usermod -aG sudo hduser
			su - hduser
			ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
			cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
 
           cd /home/hduser
           curl -O https://dlcdn.apache.org/hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz
           curl -O https://www.apache.org/dyn/closer.lua/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
		   sudo chown -R hduser:hadoop hadoop*
           sudo chown -R hduser:hadoop spark*
 
           vim /home/hduser/.bashrc 
           export HADOOP_CLASSPATH=/usr/lib/jvm/java-11-openjdkamd64/lib/tools.jar:/usr/local/hadoop/bin/hadoop
           export HADOOP_MAPRED_HOME=/usr/local/hadoop
           export HADOOP_HDFS_HOME=/usr/local/hadoop
           export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
           export PATH=$PATH:.:/usr/lib/jvm/java-11-openjdk-amd64/bin:/usr/local/hadoop/bin
           export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
           export LD_LIBRARY_PATH=/usr/local/hadoop/lib/native:$LD_LIBRARY_PATH
           export SPARK_HOME=/usr/local/spark
           export PYSPARK_PYTHON=python3
           export PATH=$SPARK_HOME/bin:$PATH
 
        source /home/hduser/.bashrc 
		hadoop-env.sh file
        add "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64"  

                            mkdir ~/tmp
                            mkdir ~/hdfs
                            chmod 750 ~/hdfs
 
         format name node and start dfs and yarn 
                     cd /usr/local/hadoop
                     bin/hdfs namenode -format 
                     sbin/start-dfs.sh 
                     sbin/start-yarn.sh
 
 
            http://external_ip:9870
 
Replace and modify the conf files accordingly