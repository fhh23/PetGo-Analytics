#python installs -- all nodes
sudo pip install boto
sudo pip install redis
sudo pip install rethinkdb
sudo pip install kafka
sudo pip install boto3
sudo pip install tdigest
#rethinkdb install -- all nodes 
source /etc/lsb-release && echo "deb http://download.rethinkdb.com/apt $DISTRIB_CODENAME main" | sudo tee /etc/apt/sources.list.d/rethinkdb.list
wget -qO- https://download.rethinkdb.com/apt/pubkey.gpg | sudo apt-key add -
sudo apt-get update
sudo apt-get install rethinkdb
#redis install -- only on master
wget http://download.redis.io/redis-stable.tar.gz
tar xvzf redis-stable.tar.gz
cd redis-stable
make
sudo apt-get install -y tcl
make test