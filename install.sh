#!/usr/bin/env bash
# Welcome!
echo "Downloading BigUtrecht and dependencies! Thanks for watching, Ioannis!"

# Set Spark environment
export SPARK_HOME=/mnt/spark-2.0.0-bin-hadoop2.7
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.1-src

# Download and install PROJ
cd ~
wget http://download.osgeo.org/proj/proj-4.9.3.tar.gz
tar xvfz proj-4.9.3.tar.gz
cd proj-4.9.3
mkdir build
./configure --prefix=/usr/
make
make install

# Download and install GDAL
cd ~
wget "http://download.osgeo.org/gdal/2.1.0/gdal-2.1.0.tar.gz"
tar -zxvf gdal-2.1.0.tar.gz
cd gdal-2.1.0
./configure --prefix=/usr/
make
sudo make install
cd swig/python/
sudo python setup.py install

# Download and install Pydoop
export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64
sudo -E pip install pydoop

# Download and install Folium
sudo -E pip install Folium

# Clone and run BigUtrecht
cd ~
rm -R -f BigUtrecht
mkdir BigUtrecht
cd BigUtrecht
git clone https://github.com/BigUtrecht/BigUtrecht.git
python BigUtrecht/app/app.py



