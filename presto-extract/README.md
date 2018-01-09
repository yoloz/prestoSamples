数据抽取及流式数据处理(一个通用的连接器).

样例在JDBC连接器上修改而来,创建的数据存在mysql中

CREATE TABLE xx WITH(xxx) AS SELECT * FROM xxx;

实现:CREATE TABLE xx WITH (xxx),如:
create table extract.default.test(id integer,name varchar) with
(location = 'jdbc:mysql://x.x.x.x:3306/',
map_json='{"user":"t","password":"t","useUnicode":"true","characterEncoding":"utf8","useSSL":"false"}'
);扩充kafka时,location可写kafka的server地址,后面json定义额外的配置;

AS SELECT * FROM xxx 未实现；




