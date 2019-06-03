# dynamodb-to-csv

## 描述
 参考[AWS DynamoDBtoCSV](https://github.com/edasque/DynamoDBtoCSV)并经过改良，可将大规模dynamodb数据保存到指定的csv文件

## 使用

```shell
node app -h
Usage: app [options]

Options:
  -V, --version                            output the version number
  -q, --query [query]                      查询条件
  -f, --output [output]                    输出文件
  -r, --region [region]                    数据库区域
  -e, --endpoint [endpoint]                数据库端口
  -t, --tableName [tableName]              数据表名称
  -s, --sslEnabled [sslEnabled]            数据库加密
  -a, --accessKeyId [accessKeyId]          数据库编号
  -k, --secretAccessKey [secretAccessKey]  数据库密钥
  -h, --help                               output usage information
```

## 依赖
- lodadh
- aws-sdk
- csv-writer
- commander

## example
``` shell
node app -r us-west-2 -t test -f test.csv
node app -r us-west-2 -a your_access_key_id -k your_secret_access_key -e your_dynamodb_endpoint -t your_dynamedb_table_name -f your_output_file_name
```


