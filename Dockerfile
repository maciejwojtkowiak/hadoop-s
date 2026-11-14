FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Install Java, Python, SSH, wget
RUN apt-get update && apt-get install -y \
    openjdk-8-jdk \
    python3 \
    python3-pip \
    ssh \
    rsync \
    wget \
    curl \
    net-tools \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# ---- Install Hadoop 3.3.6 ----
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt/ && \
    mv /opt/hadoop-${HADOOP_VERSION} $HADOOP_HOME && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

# ---- Install Hive 3.1.3 ----
ENV HIVE_VERSION=3.1.3
ENV HIVE_HOME=/opt/hive
ENV PATH=$HIVE_HOME/bin:$PATH

RUN wget -q https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz && \
    tar -xzf apache-hive-${HIVE_VERSION}-bin.tar.gz -C /opt/ && \
    mv /opt/apache-hive-${HIVE_VERSION}-bin $HIVE_HOME && \
    rm apache-hive-${HIVE_VERSION}-bin.tar.gz

# Fix Guava version conflict
RUN rm -f $HIVE_HOME/lib/guava-19.0.jar && \
    cp $HADOOP_HOME/share/hadoop/hdfs/lib/guava-*.jar $HIVE_HOME/lib/ 2>/dev/null || true

# ---- SSH config (for Hadoop pseudo-distributed) ----
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys

RUN echo "Host *\n  StrictHostKeyChecking no\n  UserKnownHostsFile=/dev/null" > ~/.ssh/config

# ---- Hadoop config ----
RUN echo '<?xml version="1.0"?>\n\
<configuration>\n\
  <property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property>\n\
</configuration>' > $HADOOP_CONF_DIR/core-site.xml

RUN echo '<?xml version="1.0"?>\n\
<configuration>\n\
  <property><name>dfs.replication</name><value>1</value></property>\n\
  <property><name>dfs.namenode.name.dir</name><value>/hadoop/dfs/name</value></property>\n\
  <property><name>dfs.datanode.data.dir</name><value>/hadoop/dfs/data</value></property>\n\
</configuration>' > $HADOOP_CONF_DIR/hdfs-site.xml

RUN echo '<?xml version="1.0"?>\n\
<configuration>\n\
  <property><name>mapreduce.framework.name</name><value>yarn</value></property>\n\
  <property><name>mapreduce.application.classpath</name>\n\
    <value>$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*</value>\n\
  </property>\n\
  <property><name>yarn.app.mapreduce.am.env</name><value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value></property>\n\
  <property><name>mapreduce.map.env</name><value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value></property>\n\
  <property><name>mapreduce.reduce.env</name><value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value></property>\n\
</configuration>' > $HADOOP_CONF_DIR/mapred-site.xml

RUN echo '<?xml version="1.0"?>\n\
<configuration>\n\
  <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>\n\
  <property><name>yarn.resourcemanager.hostname</name><value>localhost</value></property>\n\
  <property><name>yarn.nodemanager.env-whitelist</name>\n\
    <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value>\n\
  </property>\n\
</configuration>' > $HADOOP_CONF_DIR/yarn-site.xml

RUN echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_CONF_DIR/hadoop-env.sh && \
    echo "export HDFS_NAMENODE_USER=root" >> $HADOOP_CONF_DIR/hadoop-env.sh && \
    echo "export HDFS_DATANODE_USER=root" >> $HADOOP_CONF_DIR/hadoop-env.sh && \
    echo "export HDFS_SECONDARYNAMENODE_USER=root" >> $HADOOP_CONF_DIR/hadoop-env.sh && \
    echo "export YARN_RESOURCEMANAGER_USER=root" >> $HADOOP_CONF_DIR/hadoop-env.sh && \
    echo "export YARN_NODEMANAGER_USER=root" >> $HADOOP_CONF_DIR/hadoop-env.sh

# Format HDFS
RUN hdfs namenode -format -force 2>/dev/null

# Copy project files
COPY data/ /opt/data/
COPY mapreduce/mapper.py /opt/mapreduce/mapper.py
COPY mapreduce/reducer.py /opt/mapreduce/reducer.py
COPY hive/ /opt/hive_scripts/
COPY scripts/ /opt/scripts/

RUN chmod +x /opt/scripts/*.sh /opt/mapreduce/*.py

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 9870 8088 9864 10002

ENTRYPOINT ["/entrypoint.sh"]
