'use strict';

const Utils = require('../../utils');
const debug = Utils.getLogger().debugContext('sql:oracle');
const Promise = require('../../promise');
const QueryTypes = require('../../query-types');
const AbstractQuery = require('../abstract/query');
const sequelizeErrors = require('../../errors.js');
const parserStore = require('../parserStore')('oracle');
const _ = require('lodash');
const oracledb = require('oracledb');

class Query extends AbstractQuery {
  constructor(connection, sequelize, options) {
    super();
    this.connection = connection;
    this.instance = options.instance;
    this.model = options.model;
    this.sequelize = sequelize;
    this.options = _.extend({
      logging: console.log,
      plain: false,
      raw: false
    }, options || {});

    this.checkLoggingOption();

    this.maxRows = options.maxRows || 100;
    this.outFormat = options.outFormat || this.sequelize.connectionManager.lib.OBJECT;
    this.autoCommit = (options.autoCommit === false ? false : true);
  }

  getInsertIdField() {
    return 'id';
  }

  getSQLTypeFromJsType(value) {
    if (typeof value === 'number') {
      return oracledb.NUMBER;
    } else if (value instanceof Date) {
      return oracledb.DATE;
    }
    return oracledb.STRING;
  }

  _run(connection, sql, parameters) {
    this.sql = sql;

    //do we need benchmark for this query execution
    const benchmark = this.sequelize.options.benchmark || this.options.benchmark;
    let queryBegin;
    if (benchmark) {
      queryBegin = Date.now();
    } else {
      this.sequelize.log('Executing (' + (this.connection.uuid || 'default') + '): ' + this.sql, this.options);
    }

    debug(`executing(${this.connection.uuid || 'default'}) : ${this.sql}`);

    return new Promise((resolve, reject) => {
      // TRANSACTION SUPPORT
      if (_.startsWith(this.sql, 'BEGIN TRANSACTION')) {
        connection.beginTransaction(err => {
          if (err) {
            reject(this.formatError(err));
          } else {
            resolve(this.formatResults());
          }
        }, this.options.transaction.name, Utils.mapIsolationLevelStringToTedious(this.options.isolationLevel, connection.lib));
      } else if (_.startsWith(this.sql, 'COMMIT TRANSACTION')) {
        connection.commitTransaction(err => {
          if (err) {
            reject(this.formatError(err));
          } else {
            resolve(this.formatResults());
          }
        });
      } else if (_.startsWith(this.sql, 'ROLLBACK TRANSACTION')) {
        connection.rollbackTransaction(err => {
          if (err) {
            reject(this.formatError(err));
          } else {
            resolve(this.formatResults());
          }
        }, this.options.transaction.name);
      } else if (_.startsWith(this.sql, 'SAVE TRANSACTION')) {
        connection.saveTransaction(err => {
          if (err) {
            reject(this.formatError(err));
          } else {
            resolve(this.formatResults());
          }
        }, this.options.transaction.name);
      } else {
        let bindings = [];
        if (parameters) {
          bindings = _.mapValues(parameters, value =>
            _.isPlainObject(value)
              ? value
              : {
                dir: oracledb.BIND_IN,
                val: value,
                type: this.getSQLTypeFromJsType(value)
              }
          );
        }
        const cb = (err, results, fields) => {
          if (err) {
            // console.log(self.sql);
            // console.error(err.message);
            err.sql = sql;

            reject(this.formatError(err));
          } else {
            resolve(this.formatResults(results));
          }
        };
        connection.execute(
          sql,
          bindings,
          { maxRows: this.maxRows, outFormat: this.outFormat, autoCommit: this.autoCommit },
          cb
        );
      }
    });
  }

  run(sql, parameters) {
    return Promise.using(this.connection.lock(), connection => this._run(connection, sql, parameters));
  }

  static formatBindParameters(sql, values, dialect) {
    const bindParam = {};
    let i = 0;
    const seen = {};
    const vs = _.clone(values);
    const replacementFunc = (match, key, values) => {
      if (seen[ key ] !== undefined) {
        return seen[ key ];
      }
      if (values[ key ] !== undefined) {
        i = i + 1;
        bindParam[ key ] = values[ key ];
        seen[ key ] = '$' + i;
        vs[ key ] = undefined;
        return ':' + key;
      }
      return undefined;
    };
    sql = AbstractQuery.formatBindParameters(sql, values, dialect, replacementFunc)[ 0 ];

    return [ sql, bindParam ];
  }

  /**
   * High level function that handles the results of a query execution.
   *
   *
   * Example:
   *  query.formatResults([
   *    {
   *      id: 1,              // this is from the main table
   *      attr2: 'snafu',     // this is from the main table
   *      Tasks.id: 1,        // this is from the associated table
   *      Tasks.title: 'task' // this is from the associated table
   *    }
   *  ])
   *
   * @param {Array} data - The result of the query execution.
   * @private
   */
  formatResults(data) {
    var result = this.instance;

    // if (data && typeof data.rows === 'object' && typeof data.metaData === 'object' ) {


    //   var rows=[], drows=data.rows, dmeta=data.metaData
    //   var endRows=drows.length;
    //   var endMeta=dmeta.length;
    //   for (var i = 0; i < endRows; i++){
    //     var obj={}
    //     for(var j = 0 ; j < endMeta; j++){
    //        obj[dmeta[j].name]=drows[i][j];

    //     }
    //     rows.push(obj);
    //   }

    //   data={
    //     metaData: data.metaData,
    //     outBinds: data.outBinds,
    //     rows: rows,
    //     rowsAffected: data.rowsAffected
    //   };
    // }

    if (this.isInsertQuery(data)) {
      result = this.handleInsertQuery(data);
    } else if (this.isSelectQuery()) {
      result = this.handleSelectQuery(data.rows);
    } else if (this.isShowTablesQuery()) {
      result = this.handleShowTablesQuery(data.rows);
      // } else if (this.isDescribeQuery()) {
      //   result = {};

      //   data.forEach(function(_result) {
      //     result[_result.Field] = {
      //       type: _result.Type.toUpperCase(),
      //       allowNull: (_result.Null === 'YES'),
      //       defaultValue: _result.Default
      //     };
      //   });
      // } else if (this.isShowIndexesQuery()) {
      //   result = this.handleShowIndexesQuery(data);

      // } else if (this.isCallQuery()) {
      //   result = data[0];
      // } else if (this.isBulkUpdateQuery() || this.isBulkDeleteQuery() || this.isUpsertQuery()) {
      //   result = data.affectedRows;
    } else if (this.isBulkDeleteQuery() || this.isDeleteQuery()) {
      result = this.handleDeleteQuery(data);
    } else if (this.isUpdateQuery()) {
      result = this.handleUpdateQuery(data);
    } else if (this.isBulkUpdateQuery()) {
      result = this.handleBulkUpdateQuery(data);
    } else if (this.isVersionQuery()) {
      var drows = data.rows;
      var endRows = drows.length;
      for (var i = 0; i < endRows; i++) {
        if (drows[ i ].PRODUCT.indexOf('Database') >= 0) {
          result = 'PRODUCT=' + drows[ i ].PRODUCT + ', VERSION=' + drows[ i ].VERSION + ', STATUS=' + drows[ i ].STATUS;
        }
      }
      // } else if (this.isForeignKeysQuery()) {
      //   result = data;
    } else if (this.isRawQuery()) {
      // MySQL returns row data and metadata (affected rows etc) in a single object - let's standarize it, sorta
      result = [ data.rows, data ];
    }

    return result;
  }

  handleShowTablesQuery(results) {
    return results.map(resultSet => {
      return {
        tableName: resultSet.TABLE_NAME,
        schema: resultSet.TABLE_SCHEMA
      };
    });
  }

  handleShowConstraintsQuery(data) {
    //Convert snake_case keys to camelCase as it's generated by stored procedure
    return data.slice(1).map(result => {
      const constraint = {};
      for (const key in result) {
        constraint[ _.camelCase(key) ] = result[ key ];
      }
      return constraint;
    });
  }

  formatError(err) {
    return new sequelizeErrors.DatabaseError(err);
  }

  isShowOrDescribeQuery() {
    let result = false;

    result = result || this.sql.toLowerCase().indexOf('select c.column_name as \'name\', c.data_type as \'type\', c.is_nullable as \'isnull\'') === 0;
    result = result || this.sql.toLowerCase().indexOf('select tablename = t.name, name = ind.name,') === 0;
    result = result || this.sql.toLowerCase().indexOf('exec sys.sp_helpindex @objname') === 0;

    return result;
  }

  isShowIndexesQuery() {
    return this.sql.toLowerCase().indexOf('exec sys.sp_helpindex @objname') === 0;
  }

  isDeleteQuery() {
    return this.options.type === QueryTypes.DELETE;
  }

  handleShowIndexesQuery(data) {
    // Group by index name, and collect all fields
    data = _.reduce(data, (acc, item) => {
      if (!(item.index_name in acc)) {
        acc[ item.index_name ] = item;
        item.fields = [];
      }

      _.forEach(item.index_keys.split(','), column => {
        let columnName = column.trim();
        if (columnName.indexOf('(-)') !== -1) {
          columnName = columnName.replace('(-)', '');
        }

        acc[ item.index_name ].fields.push({
          attribute: columnName,
          length: undefined,
          order: column.indexOf('(-)') !== -1 ? 'DESC' : 'ASC',
          collate: undefined
        });
      });
      delete item.index_keys;
      return acc;
    }, {});

    return _.map(data, item => ({
      primary: item.index_name.toLowerCase().indexOf('pk') === 0,
      fields: item.fields,
      name: item.index_name,
      tableName: undefined,
      unique: item.index_description.toLowerCase().indexOf('unique') !== -1,
      type: undefined
    }));
  }

  handleInsertQuery(results, metaData) {
    if (this.instance) {
      // add the inserted row id to the instance
      const autoIncrementAttribute = this.model.autoIncrementAttribute;
      let id = null;

      if (
        results &&
        results.outBinds &&
        results.outBinds[ 'rid' ] &&
        results.outBinds[ 'rid' ][ 0 ]
      ) {
        id = results.outBinds[ 'rid' ][ 0 ];
      }
      this.instance[ autoIncrementAttribute ] = id;
      return [ this.instance ];
    }
  }

  handleDeleteQuery(results) {
    if (
      results &&
      results.outBinds &&
      results.outBinds[ 'affectedRows' ] &&
      results.outBinds[ 'affectedRows' ][ 0 ]
    ) {
      const affectedRows = results.outBinds[ 'affectedRows' ][ 0 ];
      return affectedRows;
    }
  }

  handleUpdateQuery(results) {
    if (this.instance) {
      if (
        results &&
        results.outBinds &&
        results.outBinds[ 'affectedRows' ] &&
        results.outBinds[ 'affectedRows' ][ 0 ]
      ) {
        return [ this.instance, results.outBinds[ 'affectedRows' ][ 0 ] ];
      }
    }
  }

  handleBulkUpdateQuery(results) {
    if (
      results &&
      results.outBinds &&
      results.outBinds[ 'affectedRows' ] &&
      results.outBinds[ 'affectedRows' ][ 0 ]
    ) {
      return results.outBinds[ 'affectedRows' ][ 0 ];
    }
  }
}

module.exports = Query;
module.exports.Query = Query;
module.exports.default = Query;
