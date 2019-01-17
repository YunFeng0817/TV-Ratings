实验环境的配置过程:
系统：Ubuntu 18.04.1 LTS
开发的IDE: IntelliJ IDEA 2018.2.6 linux版
- 在官网下载并安装,没有坑

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
安装官网的安装步骤,正常解压并安装,没有坑

运行项目:
首先用IDEA 开发软件打开项目,点开gradle的面板,刷新gradle的状态,然后一直等待gradle下载各种资源和依赖的库文件
直到其显示build success 为止
当build完成后,就可以在./src/main/java/main.java 文件中,点击运行按钮或者右键点击运行按钮,程序就会在控制台中执行,同时伴随着大量的过程和结果的输出。由于运行时间较长(大约半小时到一小时),建议耐心等待,直到程序全部执行完正常退出。

注意事项:
项目除去原始数据为 1G大小,需要预先准备足够的磁盘空间
原始数据默认不会被读取执行(因为已将解析的结果保存成文件,无需再次解析,而且解析过程非常漫长,长达8小时,不建议尝试),解析的过程在main.java中的readData()函数,在main()函数中已将其注释掉,如果想要运行该解析程序,请自行去掉注释,并且把原始文件的路径由 ./data 替换为实际的原始数据路径