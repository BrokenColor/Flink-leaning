# Flink01-start
##运行
运行该程序需要一个工具nc
> window下使用nc
1. 下载[netcat](https://eternallybored.org/misc/netcat/)
2. 将解压后的单个文件全部拷贝到C:\Windows\System32的文件夹下
Tips：注意不是拷贝整个文件夹，而是文件夹里面的全部文件。
3. 打开命令行即可使用nc.
命令行下运行：
`nc -l -p 9000`
+ -l: 用于指定nc将处于侦听模式
+ -p: 指定端口
> linux下使用nc </b>
    待补充

运行FlinkMain程序
*注意：先运行nc，然后运行FlinkMain程序*