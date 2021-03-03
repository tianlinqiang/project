# redis project

运行：
ubuntu@k8s-master:~$ ./redis-analyzers
请在脚本名后面输入 $Host [健康检查：checkup | 应用：apps]

ubuntu@k8s-master:~$ ./redis-analyzers 127.0.0.1 <checkup|apps>




打包：

1.腾讯云上面，root  
2.需要安装下载:PyInstaller-3.4.tar.gz  
安装方式  python setup.py install   (root用户)
3.构建项目包，将配置文件，代码放进去。

4.安装pip依赖包到datas里面（或许不用这么搞，只需安装即可，不一定安装在datas里面，具体没有测试）

pip install configparser --target datas/
export PYTHONPATH=$PYTHONPATH:/root/redis-analyzers/datas

5.打包
pyinstaller -F redis-analyzers.py

6.打包完成：

