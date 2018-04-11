# go-mysql
fork from [go-mysql](https://github.com/siddontang/go-mysql)

## change
- 因为更改了源码，所以把import从绝对路径改成了相对路径
- 去除了从本地执行dump命令的逻辑
- 当不指定binlog位置时，默认从最新binlog开始读
- 添加Row，RowEvent中包含[]Row，Row中包含[]TableColumn


## todo
- 持久化binlog位置