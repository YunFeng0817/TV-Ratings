### Introduction
This is a project based on a realistic big data situation that given 10Gb size of TV ratings data to analise valuable information.

The project use a big data computing framework called Spark.

Java is used as the main programming language.

Recommend Intellij IDEA as develop environment

### Warning: the size of this project is more than 1Gb,please consider the cost of network traffic before clone this project 

项目环境的配置过程:
OS ：Ubuntu 18.04.1 LTS
IDE: IntelliJ IDEA 2018.2.6 linux version

配置过程:
安装java8:
```
sudo apt-get install openjdk-8-jdk
```
安装hadoop:
Hadoop 版本: 3.1.1
官网下载
安装过程:
首先必须保证有如下两个软件
```
  $ sudo apt-get install ssh
  $ sudo apt-get install rsync
```
Unpack the downloaded Hadoop distribution. In the distribution, edit the file etc/hadoop/hadoop-env.sh to define some parameters as follows:
```
 # set to the root of your Java installation
  export JAVA_HOME=/usr/java/latest
```

安装spark
spark 版本i 2.4.0
官网下载
按照官网的安装步骤

运行项目:
用 IDEA 打开项目,点开gradle的面板,刷新gradle的状态,然后一直等待gradle下载各种资源和依赖的库文件
直到其显示build success 为止
当build完成后,就可以在./src/main/java/main.java 文件中,点击运行按钮或者右键点击运行按钮,程序就会在控制台中执行,同时伴随着大量的过程和结果的输出。由于运行时间较长(大约半小时到一小时),建议耐心等待,直到程序全部执行完正常退出。

注意事项:
项目除去原始数据为 1G大小,需要预先准备足够的磁盘空间

项目中电视节目类型对应的编号

| 节目类型 | 编号 |
| ------ | ------ |
| 儿童 | 1 |
| 综艺 | 2 |
| 电视剧 | 3 | 
| 电影 | 4 |
| 体育 | 5 |
| 新闻 | 6 | 
| 法制 | 7 |
| 财经 | 8 |
| 戏曲 | 9 |
| 购物 | 10 |
| 纪录片 | 11 |
| 科教 | 12 |
| 音乐 | 13 |
| 军事 | 14 |
| 道德文化 | 15 |

