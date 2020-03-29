# DataSocket自动化测试


## 安装方式

本项目在以下代码托管网站同步更新:
* Github：https://github.com/Raymond888/AutoTest.git

### 获取代码

    * git clone https://github.com/Raymond888/AutoTest.git
    * git checkout -b develop origin/develop


### 安装依赖

    pip install -r requirements.txt


### 配置、运行方式一
+ 在`config/keytab`中添加相应用户的认证文件
+ 将相应`settings`中`BASE_URL`、`TENANT_ID`更新为实时数据
+ 在`settings.py`中开启相应的环境配置，其他注释掉
+ 将`settings`中各类数据库以及测试数据配置正确，并检查测试数据是否正确
+ 运行`python run.py`


### 配置、运行方式二
+ 在`config/keytab`中添加相应用户的认证文件
+ 运行`python run.py -h`查看帮助信息, 按照格式添加参数
```
usage: python run.py [-h] [-b BASEURL] [-t TICKET] [-i TENANTID]

optional arguments:
  -h, --help            show this help message and exit
  -b BASEURL, --baseurl BASEURL test environment
  -i TENANTID, --tenantid TENANTID tenant id
```

### 配置、运行方式三
+ 提交代码
```
git add xxx
git commit -m "xxx"
git push origin
```
+ 配置CI/CD任务
+ 在Build with Parameters中设置baseURL、tenantId
+ 开始构建

### 配置、运行方式四
+ 在本地制作docker镜像
```
docker build -t <rep>/<name>:<tag> .
```
+ 或者拉取镜像(仅限于相应镜像仓库存在该镜像)
```
docker pull <rep>/<name>:<tag>
```
+ 运行容器示例（传入的各参数值使用最新）
```
docker run -e "baseurl=http://10.200.64.140:4000" -e "tenantid=8895c320b899447c845fc8639127f4b4" -it -v <localpath>:<mountpath <rep>/<name>:<tag>
```


