const path = require('path');
const _ = require('lodash');
const AWS = require('aws-sdk');
const program = require('commander');
const csvWriter = require('csv-writer');

const SCAN_LIMIT = 1;
const TEN_THOUSAND = 10000;

function isObject (value) { return value !== null && typeof value === 'object'; }

function changeToObject (value) { return AWS.DynamoDB.Converter.unmarshall(value); }

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
    } catch (error) {
      console.dir(error);
    }
  }
}

class Task {
  constructor () {
    this.command = {};
    this.dynamodb = {};
    this.itemCount = 0;
    this.csvWriter = {};
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

  async _getCsvFields (itemCount) {
    let csvFields = [];
    if (itemCount < TEN_THOUSAND) {
      let result = {};
      let scanCount = 0;
      let scanOption = { TableName: this.command.tableName, Limit: SCAN_LIMIT };
      do {
        result = await this.dynamodb.scanTable(scanOption);
        const fields = result.Items.map(item => Object.keys(item)).reduce((accumulator, currentValue) => _.union(accumulator, currentValue), []);
        csvFields = _.union(csvFields, fields);
        if (result.LastEvaluatedKey) {
          scanCount += SCAN_LIMIT;
          scanOption.ExclusiveStartKey = result.LastEvaluatedKey;
        }
      } while (result.LastEvaluatedKey && scanCount < TEN_THOUSAND);
    }
    return csvFields;
  }

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

  async check () {
    const tableName = this.command.tableName;
    const tableDesc = await this.dynamodb.describeTable({ TableName: tableName });
    if (!tableDesc) throw new Error(`数据表${tableName}不存在`);
    const itemCount = tableDesc.Table.ItemCount;
    if (itemCount < 1) throw new Error(`数据表${tableName}不存在数据`);
    console.log(`数据表${tableName}共包含${itemCount}条数据`);
    this.itemCount = itemCount;
  }

  async initCsv () {
    const tableName = this.command.tableName;
    const csvFields = await this._getCsvFields(this.itemCount);
    console.log(`数据表${tableName}列名包括：${csvFields.join(', ')}`);
    const output = path.join(__dirname, this.command.output);
    this.csvWriter = csvWriter.createObjectCsvWriter({
      path: output,
      header: csvFields.map(field => { return { id: field, title: field }; })
    });
  }

  async saveCsv () {
    let result = {};
    let scanCount = 0;
    let scanOption = { TableName: this.command.tableName, Limit: SCAN_LIMIT };
    do {
      console.log(`正在处理第${scanCount}条数据`);
      result = await this.dynamodb.scanTable(scanOption) || {};
      const items = result.Items.map(dynamodbItem => {
        let objectItem = changeToObject(dynamodbItem);
        for (let key in objectItem) {
          if (isObject(objectItem[key])) objectItem[key] = JSON.stringify(objectItem[key]);
        }
        return objectItem;
      });
      if (items.length > 0) await this.csvWriter.writeRecords(items);
      if (result.LastEvaluatedKey) scanOption.ExclusiveStartKey = result.LastEvaluatedKey;
      scanCount += SCAN_LIMIT;
    } while (result.LastEvaluatedKey);
    console.log('处理完成');
  }

  async execute () {
    try {
      await this.check();
      await this.initCsv();
      await this.saveCsv();
    } catch (error) {
      console.log('出现错误');
      console.log(error);
    }
  }
}

const task = new Task();
task.init();
task.execute();

module.exports = new Task();
