const path = require('path');
const _ = require('lodash');
const AWS = require('aws-sdk');
const program = require('commander');
const csvWriter = require('csv-writer');

const SCAN_LIMIT = 1000;
// 一万数据量
const TEN_THOUSAND = 10000;
// 五万数据量
// const FIFTY_THOUSAND = 50000;
// 十万数据量
// const ONE_HUNDRED_THOUSAND = 100000;
// 一百万数据量
// const ONE_MILLION = 1000000;

// const createCsvWriter = require('csv-writer').createObjectCsvWriter;

// const converter = AWS.DynamoDB.Converter;

// function isObject (value) { return value !== null && typeof value === 'object'; }

class DynamoDB {
  constructor (config = {}) {
    const { region, endpoint, sslEnabled, accessKeyId, secretAccessKey } = config;
    this.fieldsOfItem = [];
    this.valuesOfItem = [];
    if (region) AWS.config.update({ region });
    if (endpoint) AWS.config.update({ endpoint });
    if (sslEnabled) AWS.config.update({ sslEnabled });
    if (accessKeyId) AWS.config.update({ accessKeyId });
    if (secretAccessKey) AWS.config.update({ secretAccessKey });
    this.dynamodb = new AWS.DynamoDB();
  }

  // _cannotFoundFiled (field) { return this.fieldsOfItem.indexOf(field) < 0; }

  // formatItems (items = []) {
  //   if (items.length < 1) return;
  //   let fieldsOfItem = [];
  //   let valuesOfItem = [];

  //   for (let item of items) {
  //     let row = {};
  //     let itemOfJsObject = converter.unmarshall(item);
  //     Object.keys(itemOfJsObject).forEach(field => {
  //       // csv属性
  //       if (!fieldsOfItem.includes(field)) fieldsOfItem.push(field.trim());
  //       // csv内容
  //       if (!isObject) row[field] = itemOfJsObject[field];
  //       else row[field] = JSON.stringify(itemOfJsObject[field]);
  //     });
  //     valuesOfItem.push(row);
  //   }

  //   return { fields: fieldsOfItem, values: valuesOfItem };
  // }

  async describeTable (query) {
    try {
      const result = await this.dynamodb.describeTable(query).promise();
      return result;
    } catch (error) {
      console.dir(error);
    }
  }

  async scanTable (query) {
    try {
      const result = await this.dynamodb.scan(query).promise();
      return result;
      // this._keepItems(result.Items);
      // if (result.LastEvaluatedKey) {
      //   let nextQuery = Object.assign({}, query, { ExclusiveStartKey: result.LastEvaluatedKey });
      //   return this.scanDynamoDB(nextQuery);
      // }
      // return { fields: this.fieldsOfItem, values: this.valuesOfItem };
    } catch (error) {
      console.dir(error);
    }
  }
}

class Task {
  constructor () {
    this.command = {};
    this.dynamodb = {};
    this.csvWriter = {};
    // this.csvFields = [];
    // this.csvValues = [];
  }

  _parseCommand () {
    this.command = program.version('1.0.0')
      .option('-q, --query [query]', '查询条件')
      .option('-f, --output [output]', '输出文件')
      .option('-r, --region [region]', '数据库区域')
      .option('-e, --endpoint [endpoint]', '数据库端口')
      .option('-t, --tableName [tableName]', '数据表名称')
      .option('-s, --sslEnabled [sslEnabled]', '数据库加密')
      .option('-a, --accessKeyId [accessKeyId]', '数据库编号')
      .option('-k, --secretAccessKey [secretAccessKey]', '数据库密钥')
      .parse(process.argv);
  }

  _checkCommand () {
    if (!this.command.output) throw new Error('输出文件必须设置');
    if (!this.command.region) throw new Error('数据库区域必须设置');
    if (!this.command.tableName) throw new Error('数据表名称必须设置');
  }

  // async saveCsv (fields = [], values = []) {
  //   const outputPath = path.join(__dirname, this.command.output);
  //   if (Object.keys(this.csvWriter).length < 1) {
  //     this.csvWriter = createCsvWriter({
  //       path: outputPath,
  //       header: [
  //         { id: 'uid', title: 'uid' },
  //         { id: 'bid', title: 'bid' },
  //         { id: 'cid', title: 'cid' }
  //       ]
  //     });
  //   }
  //   await this.csvWriter.writeRecords(values);
  //   // let outputValue;
  //   // if (a) outputValue = csv.unparse({ fields: fields, data: values });
  //   // else outputValue = csv.unparse({ data: values });
  //   // console.log(outputValue);
  //   // const outputValue = csv.unparse({ fields: fields, data: values });
  //   // const outputValue = csv.unparse({ data: values });
  //   // fs.appendFileSync(outputPath, outputValue);
  // }

  async init () {
    try {
      this._parseCommand();
      this._checkCommand();
      this.dynamodb = new DynamoDB(this.command);
    } catch (error) {
      console.log(error);
      this.command.outputHelp();
    }
  }

  async getCsvFields (itemCount) {
    let csvFields = [];
    if (itemCount < TEN_THOUSAND) {
      let result = {};
      let scanCount = 0;
      let scanOption = { TableName: this.command.tableName, Limit: SCAN_LIMIT };
      do {
        result = await this.dynamodb.scanTable(scanOption);
        const fields = result.Items.map(item => Object.keys(item)).reduce((accumulator, currentValue) => _.union(accumulator, currentValue));
        csvFields = _.union(csvFields, fields);
        if (result.LastEvaluatedKey) {
          scanCount += SCAN_LIMIT;
          scanOption.ExclusiveStartKey = result.LastEvaluatedKey;
        }
      } while (result.LastEvaluatedKey && scanCount < TEN_THOUSAND);
    }
    return csvFields;
  }

  createCsvWriter (output, csvFields) {
    this.csvWriter = csvWriter.createObjectCsvWriter({
      path: output,
      header: csvFields.map(field => { return { id: field, title: field }; })
    });
  }

  // async getCsvValues (scanOption) {

  //   do {
  //     result = await this.dynamodb.scanTable(scanOption);
  //     const fields = result.Items.map(item => Object.keys(item)).reduce((accumulator, currentValue) => _.union(accumulator, currentValue));
  //     if (result.LastEvaluatedKey) {
  //       scanCount += SCAN_LIMIT;
  //       scanOption.ExclusiveStartKey = result.LastEvaluatedKey;
  //     }
  //   } while (result.LastEvaluatedKey);
  // }

  async saveCsvValues () {
    let result = {};
    let scanOption = { TableName: this.command.tableName, Limit: SCAN_LIMIT };
    do {
      let items = [];
      result = await this.dynamodb.scanTable(scanOption) || {};
      result.Items.map(item => {
        let itemOfJsObject = AWS.DynamoDB.Converter.unmarshall(item);
        console.log(item);
        console.log(itemOfJsObject);
        console.log('------------');
        // Object.keys(itemOfJsObject).map(field => {
        //   if (!isObject) row[field] = itemOfJsObject[field];
        //   else row[field] = JSON.stringify(itemOfJsObject[field]);
        // });
        // valuesOfItem.push(row);
      });
      console.log(result.Items);
      if (result.LastEvaluatedKey) scanOption.ExclusiveStartKey = result.LastEvaluatedKey;
      // if (result.Items && result.Items.length > 0) await this.csvWriter.writeRecords(result.Items);
    } while (result.LastEvaluatedKey);
  }

  async execute () {
    try {
      const tableName = this.command.tableName;
      const tableDesc = await this.dynamodb.describeTable({ TableName: tableName });
      if (!tableDesc) throw new Error(`数据表${tableName}不存在`);
      const itemCount = tableDesc.Table.ItemCount;
      if (itemCount < 1) throw new Error(`数据表${tableName}不存在数据`);
      console.log(`数据表${tableName}共包含${itemCount}条数据`);
      console.log(`正在统计数据表${tableName}列名`);
      const csvFields = await this.getCsvFields(itemCount);
      console.log(`数据表${tableName}列名包括：${csvFields.join(', ')}`);
      const output = path.join(__dirname, this.command.output);
      this.createCsvWriter(output, csvFields);
      await this.saveCsvValues();
    } catch (error) {
      console.log('出现错误');
      console.log(error);
    }

    // const itemCount = tableDesc.table.ItemCount;
    // console.log(`数据表${tableName}共包含${itemCount}条数据`);
    // console.log(`------正在统计数据表${tableName}列名------`);
    // this.getCsvFields(itemCount);
  }

  // async execute (query = { TableName: 'tony-test', Limit: 1 }) {
  //   try {
  //     const result = await this.dynamodb.scanDynamoDB(query);

  //     if (result.Items.length < 1) return;
  //     const { fields, values } = this.dynamodb.formatItems(result.Items);
  //     await this.saveCsv(fields, values, true);

  //     if (!result.LastEvaluatedKey) {
  //       console.log('爬取完成');
  //     } else {
  //       let nextQuery = Object.assign({}, query, { ExclusiveStartKey: result.LastEvaluatedKey });
  //       return this.execute(nextQuery);
  //     }
  //   } catch (error) {
  //     console.dir(error);
  //   }
  // }
}

const task = new Task();
task.init();
task.execute();

module.exports = new Task();
