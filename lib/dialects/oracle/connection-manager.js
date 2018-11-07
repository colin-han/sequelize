'use strict';

const AbstractConnectionManager = require('../abstract/connection-manager');
const ResourceLock = require('./resource-lock');
const Promise = require('../../promise');
const Utils = require('../../utils');
const debug = Utils.getLogger().debugContext('connection:oracle');
const debugTedious = Utils.getLogger().debugContext('connection:oracle:oracledb');
const sequelizeErrors = require('../../errors');
const parserStore = require('../parserStore')('oracle');
const _ = require('lodash');

const DEFAULT_PORT = 1521;
class ConnectionManager extends AbstractConnectionManager {
  constructor(dialect, sequelize) {
    super(dialect, sequelize);

    this.sequelize = sequelize;
    this.sequelize.config.port = this.sequelize.config.port || DEFAULT_PORT;
    try {
      if (sequelize.config.dialectModulePath) {
        this.lib = require(sequelize.config.dialectModulePath);
      } else {
        this.lib = require('oracledb');
      }
    } catch (err) {
      if (err.code === 'MODULE_NOT_FOUND') {
        throw new Error('Please install oracledb package manually');
      }
      throw err;
    }
  }

  // Expose this as a method so that the parsing may be updated when the user has added additional, custom types
  _refreshTypeParser(dataType) {
    parserStore.refresh(dataType);
  }

  _clearTypeParser() {
    parserStore.clear();
  }

  connect(config) {
    return new Promise((resolve, reject) => {
      const host = config.host;
      const port = config.port && config.port !== DEFAULT_PORT ? ':' + config.port : '';
      const sid = '/' + ((config.dialectOptions && config.dialectOptions.sid) || 'XE');
      const connectionConfig = {
        user: config.username,
        password: config.password,
        connectString: _.join([ host, port, sid ], '')
      };

      if (config.dialectOptions) {
        for (const key of Object.keys(config.dialectOptions)) {
          connectionConfig.options[ key ] = config.dialectOptions[ key ];
        }
      }

      this.lib.getConnection(connectionConfig, (error, connection) => {
        if (error) {
          if (!error.code) {
            reject(new sequelizeErrors.ConnectionError(error));
            return;
          }

          switch (error.code) {
            case 'ESOCKET':
              if (_.includes(err.message, 'connect EHOSTUNREACH')) {
                reject(new sequelizeErrors.HostNotReachableError(err));
              } else if (_.includes(err.message, 'connect ENETUNREACH')) {
                reject(new sequelizeErrors.HostNotReachableError(err));
              } else if (_.includes(err.message, 'connect EADDRNOTAVAIL')) {
                reject(new sequelizeErrors.HostNotReachableError(err));
              } else if (_.includes(err.message, 'getaddrinfo ENOTFOUND')) {
                reject(new sequelizeErrors.HostNotFoundError(err));
              } else if (_.includes(err.message, 'connect ECONNREFUSED')) {
                reject(new sequelizeErrors.ConnectionRefusedError(err));
              } else {
                reject(new sequelizeErrors.ConnectionError(err));
              }
              break;
            case 'ER_ACCESS_DENIED_ERROR':
            case 'ELOGIN':
              reject(new sequelizeErrors.AccessDeniedError(err));
              break;
            case 'EINVAL':
              reject(new sequelizeErrors.InvalidConnectionError(err));
              break;
            default:
              reject(new sequelizeErrors.ConnectionError(err));
              break;
          }
          return;
        }

        const connectionLock = new ResourceLock(connection);
        connection.lib = this.lib;

        if (config.dialectOptions && config.dialectOptions.debug) {
          connection.on('debug', debugTedious);
        }

        if (config.pool.handleDisconnects) {
          connection.on('error', err => {
            switch (err.code) {
              case 'ESOCKET':
              case 'ECONNRESET':
                this.pool.destroy(connectionLock)
                  .catch(/Resource not currently part of this pool/, () => {
                  });
            }
          });
        }

        resolve(connectionLock);
      });
    }).timeout(1980,'Error: timeout of 2000ms exceeded. Check your configuration and your database.');
  }

  disconnect(connectionLock) {
    const connection = connectionLock.unwrap();

    // Dont disconnect a connection that is already disconnected
    if (connection.closed) {
      return Promise.resolve();
    }

    return new Promise((resolve, reject) => {
      connection.close((error) => {
        debug('connection closed');
        if (error) {
          reject(error);
        } else {
          resolve()
        }
      });
    });
  }

  validate(connectionLock) {
    const connection = connectionLock.unwrap();
    return connection && connection.loggedIn;
  }
}

module.exports = ConnectionManager;
module.exports.ConnectionManager = ConnectionManager;
module.exports.default = ConnectionManager;
