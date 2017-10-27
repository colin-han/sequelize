'use strict';

const _ = require('lodash');
const AbstractDialect = require('../abstract');
const ConnectionManager = require('./connection-manager');
const Query = require('./query');
const QueryGenerator = require('./query-generator');
const DataTypes = require('../../data-types').oracle;

class OracleDialect extends AbstractDialect {
  constructor(sequelize) {
    super();
    this.sequelize = sequelize;
    this.connectionManager = new ConnectionManager(this, sequelize);
    this.QueryGenerator = _.extend({}, QueryGenerator, {
      options: sequelize.options,
      _dialect: this,
      sequelize
    });
  }
}

OracleDialect.prototype.supports = _.merge(_.cloneDeep(AbstractDialect.prototype.supports), {
  'DEFAULT': true,
  'DEFAULT VALUES': true,
  'LIMIT ON UPDATE': true,
  'ORDER NULLS': false,
  lock: false,
  transactions: true,
  migrations: false,
  upserts: true,
  returnValues: {
    output: true
  },
  schemas: true,
  autoIncrement: {
    identityInsert: true,
    defaultValue: false,
    update: false
  },
  constraints: {
    restrict: false,
    default: true
  },
  index: {
    collate: false,
    length: false,
    parser: false,
    type: true,
    using: false,
    where: true
  },
  NUMERIC: true,
  tmpTableTrigger: true
});

ConnectionManager.prototype.defaultVersion = '11.2.0.2.0'; // Oracle Database 11g Express Edition
OracleDialect.prototype.Query = Query;
OracleDialect.prototype.name = 'oracle';
OracleDialect.prototype.TICK_CHAR = '"';
OracleDialect.prototype.TICK_CHAR_LEFT = '[';
OracleDialect.prototype.TICK_CHAR_RIGHT = ']';
OracleDialect.prototype.DataTypes = DataTypes;

module.exports = OracleDialect;
