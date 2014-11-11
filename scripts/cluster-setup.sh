#!/bin/bash

# ========================================================================================================== #

echo "Installing Java..."

sudo apt-get install -y openjdk-7-jdk

# ========================================================================================================== #

echo "Installing Zookeeper..."

CWD=`pwd`
ZOOKEEPER_DATA="$CWD/zookeeper-data"

wget http://mirror.softaculous.com/apache/zookeeper/stable/zookeeper-3.4.5.tar.gz
tar -xf zookeeper-3.4.5.tar.gz
rm zookeeper-3.4.5.tar.gz
mkdir -p $ZOOKEEPER_DATA
rm -rf $ZOOKEEPER_DATA/*
echo -e "tickTime=2000\ndataDir=$ZOOKEEPER_DATA\nclientPort=2181" > zookeeper-3.4.5/conf/zoo.cfg
export JVMFLAGS="-Djava.net.preferIPv4Stack=true"
$CWD/zookeeper-3.4.5/bin/zkServer.sh start-foreground &

# ========================================================================================================== #

echo "Installing ZeroMQ 2.1.7..."

sudo apt-get install -y build-essential
sudo apt-get install -y uuid-dev
cd /tmp
wget http://download.zeromq.org/zeromq-2.1.7.tar.gz
tar -xzf zeromq-2.1.7.tar.gz
cd zeromq-2.1.7
./configure
make
sudo make install

#============================================================================================================ #

echo "Installing JZMQ..."

sudo apt-get install -y git libtool autoconf
sudo apt-get install -y pkg-config
cd /tmp
git clone https://github.com/cbsmith/jzmq
cd jzmq
./autogen.sh
./configure
make

#============================================================================================================ #

echo "Creating a dedicated Storm system user..."

sudo groupadd -g 53001 storm
sudo mkdir -p /app/home/storm
sudo useradd -u 53001 -g 53001 -d /app/home/storm -s /bin/bash storm -c "Storm service account"
sudo chmod 700 /app/home/storm
sudo chage -I -1 -E -1 -m -1 -M -1 -W -1 -E -1 storm

#============================================================================================================ #

echo "Downloading and installing a Storm release..."

sudo apt-get install unzip
cd /tmp
wget https://dl.dropbox.com/u/133901206/storm-0.8.2.zip
cd /usr/local
sudo unzip /tmp/storm-0.8.2.zip
sudo chown -R storm:storm storm-0.8.2
sudo ln -s storm-0.8.2 storm

#============================================================================================================ #

echo "Creating local working directory for Storm..."

sudo mkdir -p /app/storm
sudo chown -R storm:storm /app/storm


#============================================================================================================ #

# 
# ### Configuration for conf/storm.yaml file ###
# 
# storm.zookeeper.servers:
#     - "zkserver1"
# 
# nimbus.host: "nimbus1"
# nimbus.childopts: "-Xmx1024m -Djava.net.preferIPv4Stack=true"
# 
# ui.childopts: "-Xmx768m -Djava.net.preferIPv4Stack=true"
# 
# supervisor.childopts: "-Djava.net.preferIPv4Stack=true"
# worker.childopts: "-Xmx768m -Djava.net.preferIPv4Stack=true"
# 
# storm.local.dir: "/app/storm"

#============================================================================================================ #

echo "Starting the Nimbus daemon manually"

sudo su - storm
cd /usr/local/storm
bin/storm nimbus

#============================================================================================================ #

echo "Starting the Storm UI daemon manually"

bin/storm ui

#============================================================================================================ #
