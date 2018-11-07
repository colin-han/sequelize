'use strict';

const _ = require('lodash');
const Utils = require('../../utils');
const DataTypes = require('../../data-types');
const Model = require('../../model');
const AbstractQueryGenerator = require('../abstract/query-generator');
const randomBytes = require('crypto').randomBytes;
const semver = require('semver');
const SqlString = require('./sql-string');

const Op = require('../../operators');

/* istanbul ignore next */
const throwMethodUndefined = function (methodName) {
  throw new Error('The method "' + methodName + '" is not defined! Please add it to your sql dialect.');
};

function generateRandomChar() {
  const v = Math.floor(Math.random() * 52);
  return v < 26 ? v + 65 : v - 26 + 97;
}
function generateRandomString() {
  return String.fromCharCode(
    generateRandomChar(),
    generateRandomChar(),
    generateRandomChar(),
    generateRandomChar(),
    generateRandomChar()
  );
}
const QueryGenerator = {
  __proto__: AbstractQueryGenerator,
  options: {},
  dialect: 'oracle',

  // createSchema and showSchemasQuery are not support by Oracle.

  // OK
  versionQuery() {
    return 'SELECT * FROM PRODUCT_COMPONENT_VERSION';
  },

  // OK
  selectQuery(tableName, options, model) {
    // Enter and change at your own peril -- Mick Hansen

    options = options || {};

    let table = null
      , self = this
      , query
      , limit = options.limit
      , mainModel = model
      , mainQueryItems = []
      , mainAttributes = options.attributes && options.attributes.slice(0)
      , mainJoinQueries = []
      // We'll use a sub query if we have a hasMany association and a limit
      , subQuery = options.subQuery === undefined ?
      limit && options.hasMultiAssociation :
      options.subQuery
      , subQueryItems = []
      , subQueryAttributes = null
      , subJoinQueries = []
      , mainTableAs = null;

    if (options.tableAs) {
      mainTableAs = this.quoteTable(options.tableAs);
    } else if (!Array.isArray(tableName) && model) {
      options.tableAs = mainTableAs = this.quoteTable(model.name);
    }

    options.table = table = !Array.isArray(tableName) ? this.quoteTable(tableName) : tableName.map(function (t) {
      if (Array.isArray(t)) {
        return this.quoteTable(t[ 0 ], t[ 1 ]);
      }
      return this.quoteTable(t, true);
    }.bind(this)).join(', ');

    if (subQuery && mainAttributes) {
      model.primaryKeyAttributes.forEach(function (keyAtt) {
        // Check if mainAttributes contain the primary key of the model either as a field or an aliased field
        if (!_.find(mainAttributes, function (attr) {
          return keyAtt === attr || keyAtt === attr[ 0 ] || keyAtt === attr[ 1 ];
        })) {
          mainAttributes.push(model.rawAttributes[ keyAtt ].field ? [ keyAtt, model.rawAttributes[ keyAtt ].field ] : keyAtt);
        }
      });
    }

    // Escape attributes
    mainAttributes = this.escapeAttributes(mainAttributes, options, mainTableAs);

    // If no attributes specified, use *
    mainAttributes = mainAttributes || (options.include ? [ mainTableAs + '.*' ] : [ '*' ]);

    // If sub query, we ad the mainAttributes to the subQuery and set the mainAttributes to select * from subquery
    if (subQuery) {
      // We need primary keys
      subQueryAttributes = mainAttributes;
      mainAttributes = [ mainTableAs + '.*' ];
    }

    if (options.include) {
      let generateJoinQueries = function (include, parentTable) {
        let table = include.model.getTableName()
          , as = include.as
          , joinQueryItem = ''
          , joinQueries = {
          mainQuery: [],
          subQuery: []
        }
          , attributes
          , association = include.association
          , through = include.through
          , joinType = include.required ? ' INNER JOIN ' : ' LEFT OUTER JOIN '
          , whereOptions = _.clone(options)
          , targetWhere;

        whereOptions.keysEscaped = true;

        if (tableName !== parentTable && mainTableAs !== parentTable) {
          as = parentTable + '.' + include.as;
        }

        // includeIgnoreAttributes is used by aggregate functions
        if (options.includeIgnoreAttributes !== false) {

          attributes = include.attributes.map(function (attr) {
            let attrAs = attr,
              verbatim = false;

            if (Array.isArray(attr) && attr.length === 2) {
              if (attr[ 0 ]._isSequelizeMethod) {
                if (attr[ 0 ] instanceof Utils.literal ||
                  attr[ 0 ] instanceof Utils.cast ||
                  attr[ 0 ] instanceof Utils.fn
                ) {
                  verbatim = true;
                }
              }

              attr = attr.map(function ($attr) {
                return $attr._isSequelizeMethod ? self.handleSequelizeMethod($attr) : $attr;
              });

              attrAs = attr[ 1 ];
              attr = attr[ 0 ];
            } else if (attr instanceof Utils.Literal) {
              return attr.val; // We trust the user to rename the field correctly
            } else if (attr instanceof Utils.Cast ||
              attr instanceof Utils.Fn
            ) {
              throw new Error(
                'Tried to select attributes using Sequelize.cast or Sequelize.fn without specifying an alias for the result, during eager loading. ' +
                'This means the attribute will not be added to the returned instance'
              );
            }

            let prefix;
            if (verbatim === true) {
              prefix = attr;
            } else {
              prefix = self.quoteIdentifier(as) + '.' + self.quoteIdentifier(attr);
            }
            return prefix + ' ' + self.quoteIdentifier(as + '.' + attrAs, true);
          });
          if (include.subQuery && subQuery) {
            subQueryAttributes = subQueryAttributes.concat(attributes);
          } else {
            mainAttributes = mainAttributes.concat(attributes);
          }
        }

        if (through) {
          let throughTable = through.model.getTableName()
            , throughAs = as + '.' + through.as
            , throughAttributes = through.attributes.map(function (attr) {
            return self.quoteIdentifier(throughAs) + '.' + self.quoteIdentifier(Array.isArray(attr) ? attr[ 0 ] : attr) +
              ' ' +
              self.quoteIdentifier(throughAs + '.' + (Array.isArray(attr) ? attr[ 1 ] : attr));
          })
            , primaryKeysSource = association.source.primaryKeyAttributes
            , tableSource = parentTable
            , identSource = association.identifierField
            , attrSource = primaryKeysSource[ 0 ]
            , primaryKeysTarget = association.target.primaryKeyAttributes
            , tableTarget = as
            , identTarget = association.foreignIdentifierField
            , attrTarget = association.target.rawAttributes[ primaryKeysTarget[ 0 ] ].field || primaryKeysTarget[ 0 ]

            , sourceJoinOn
            , targetJoinOn

            , throughWhere;

          if (options.includeIgnoreAttributes !== false) {
            // Through includes are always hasMany, so we need to add the attributes to the mainAttributes no matter what (Real join will never be executed in subquery)
            mainAttributes = mainAttributes.concat(throughAttributes);
          }

          // Figure out if we need to use field or attribute
          if (!subQuery) {
            attrSource = association.source.rawAttributes[ primaryKeysSource[ 0 ] ].field;
          }
          if (subQuery && !include.subQuery && !include.parent.subQuery && include.parent.model !== mainModel) {
            attrSource = association.source.rawAttributes[ primaryKeysSource[ 0 ] ].field;
          }

          // Filter statement for left side of through
          // Used by both join and subquery where

          // If parent include was in a subquery need to join on the aliased attribute
          if (subQuery && !include.subQuery && include.parent.subQuery) {
            sourceJoinOn = self.quoteIdentifier(tableSource + '.' + attrSource) + ' = ';
          } else {
            sourceJoinOn = self.quoteTable(tableSource) + '.' + self.quoteIdentifier(attrSource) + ' = ';
          }
          sourceJoinOn += self.quoteIdentifier(throughAs) + '.' + self.quoteIdentifier(identSource);

          // Filter statement for right side of through
          // Used by both join and subquery where
          targetJoinOn = self.quoteIdentifier(tableTarget) + '.' + self.quoteIdentifier(attrTarget) + ' = ';
          targetJoinOn += self.quoteIdentifier(throughAs) + '.' + self.quoteIdentifier(identTarget);

          if (include.through.where) {
            throughWhere = self.getWhereConditions(include.through.where, self.sequelize.literal(self.quoteIdentifier(throughAs)), include.through.model);
          }

          if (self._dialect.supports.joinTableDependent) {
            // Generate a wrapped join so that the through table join can be dependent on the target join
            joinQueryItem += joinType + '(';
            joinQueryItem += self.quoteTable(throughTable, throughAs);
            joinQueryItem += ' INNER JOIN ' + self.quoteTable(table, as) + ' ON ';
            joinQueryItem += targetJoinOn;

            if (throughWhere) {
              joinQueryItem += ' AND ' + throughWhere;
            }

            joinQueryItem += ') ON ' + sourceJoinOn;
          } else {
            // Generate join SQL for left side of through
            joinQueryItem += joinType + self.quoteTable(throughTable, throughAs) + ' ON ';
            joinQueryItem += sourceJoinOn;

            // Generate join SQL for right side of through
            joinQueryItem += joinType + self.quoteTable(table, as) + ' ON ';
            joinQueryItem += targetJoinOn;

            if (throughWhere) {
              joinQueryItem += ' AND ' + throughWhere;
            }

          }

          if (include.where || include.through.where) {
            if (include.where) {
              targetWhere = self.getWhereConditions(include.where, self.sequelize.literal(self.quoteIdentifier(as)), include.model, whereOptions);
              if (targetWhere) {
                joinQueryItem += ' AND ' + targetWhere;
              }
            }
            if (subQuery && include.required) {
              if (!options.where) options.where = {};
              (function (include) {
                // Closure to use sane local variables

                let parent = include
                  , child = include
                  , nestedIncludes = []
                  , topParent
                  , topInclude
                  , $query;

                while (parent = parent.parent) {
                  nestedIncludes = [ _.extend({}, child, { include: nestedIncludes }) ];
                  child = parent;
                }

                topInclude = nestedIncludes[ 0 ];
                topParent = topInclude.parent;

                if (topInclude.through && Object(topInclude.through.model) === topInclude.through.model) {
                  $query = self.selectQuery(topInclude.through.model.getTableName(), {
                    attributes: [ topInclude.through.model.primaryKeyAttributes[ 0 ] ],
                    include: [ {
                      model: topInclude.model,
                      as: topInclude.model.name,
                      attributes: [],
                      association: {
                        associationType: 'BelongsTo',
                        isSingleAssociation: true,
                        source: topInclude.association.target,
                        target: topInclude.association.source,
                        identifier: topInclude.association.foreignIdentifier,
                        identifierField: topInclude.association.foreignIdentifierField
                      },
                      required: true,
                      include: topInclude.include,
                      _pseudo: true
                    } ],
                    where: self.sequelize.and(
                      self.sequelize.asIs([
                        self.quoteTable(topParent.model.name) + '.' + self.quoteIdentifier(topParent.model.primaryKeyAttributes[ 0 ]),
                        self.quoteIdentifier(topInclude.through.model.name) + '.' + self.quoteIdentifier(topInclude.association.identifierField)
                      ].join(' = ')),
                      topInclude.through.where
                    ),
                    limit: 1,
                    includeIgnoreAttributes: false
                  }, topInclude.through.model);
                } else {
                  $query = self.selectQuery(topInclude.model.tableName, {
                    attributes: [ topInclude.model.primaryKeyAttributes[ 0 ] ],
                    include: topInclude.include,
                    where: {
                      $join: self.sequelize.asIs([
                        self.quoteTable(topParent.model.name) + '.' + self.quoteIdentifier(topParent.model.primaryKeyAttributes[ 0 ]),
                        self.quoteIdentifier(topInclude.model.name) + '.' + self.quoteIdentifier(topInclude.association.identifierField)
                      ].join(' = '))
                    },
                    limit: 1,
                    includeIgnoreAttributes: false
                  }, topInclude.model);
                }

                options.where[ '__' + throughAs ] = self.sequelize.asIs([
                  '(',
                  $query.replace(/;$/, ''),
                  ')',
                  'IS NOT NULL'
                ].join(' '));
              })(include);
            }
          }
        } else {
          let left = association.source
            , right = association.target
            , primaryKeysLeft = left.primaryKeyAttributes
            , primaryKeysRight = right.primaryKeyAttributes
            , tableLeft = parentTable
            , attrLeft = association.associationType === 'BelongsTo' ?
            association.identifierField || association.identifier :
            primaryKeysLeft[ 0 ]

            , tableRight = as
            , attrRight = association.associationType !== 'BelongsTo' ?
            association.identifierField || association.identifier :
            right.rawAttributes[ primaryKeysRight[ 0 ] ].field || primaryKeysRight[ 0 ]
            , joinOn
            , subQueryJoinOn;

          // Filter statement
          // Used by both join and where
          if (subQuery && !include.subQuery && include.parent.subQuery && (include.hasParentRequired || include.hasParentWhere || include.parent.hasIncludeRequired || include.parent.hasIncludeWhere)) {
            joinOn = self.quoteIdentifier(tableLeft + '.' + attrLeft);
          } else {
            if (association.associationType !== 'BelongsTo') {
              // Alias the left attribute if the left attribute is not from a subqueried main table
              // When doing a query like SELECT aliasedKey FROM (SELECT primaryKey FROM primaryTable) only aliasedKey is available to the join, this is not the case when doing a regular select where you can't used the aliased attribute
              if (!subQuery || (subQuery && include.parent.model !== mainModel)) {
                if (left.rawAttributes[ attrLeft ].field) {
                  attrLeft = left.rawAttributes[ attrLeft ].field;
                }
              }
            }
            joinOn = self.quoteTable(tableLeft) + '.' + self.quoteIdentifier(attrLeft);
          }
          subQueryJoinOn = self.quoteTable(tableLeft) + '.' + self.quoteIdentifier(attrLeft);

          joinOn += ' = ' + self.quoteTable(tableRight) + '.' + self.quoteIdentifier(attrRight);
          subQueryJoinOn += ' = ' + self.quoteTable(tableRight) + '.' + self.quoteIdentifier(attrRight);

          if (include.where) {
            targetWhere = self.getWhereConditions(include.where, self.sequelize.literal(self.quoteIdentifier(as)), include.model, whereOptions);
            if (targetWhere) {
              joinOn += ' AND ' + targetWhere;
              subQueryJoinOn += ' AND ' + targetWhere;
            }
          }

          // If its a multi association and the main query is a subquery (because of limit) we need to filter based on this association in a subquery
          if (subQuery && association.isMultiAssociation && include.required) {
            if (!options.where) options.where = {};
            // Creating the as-is where for the subQuery, checks that the required association exists
            let $query = self.selectQuery(include.model.getTableName(), {
              tableAs: as,
              attributes: [ attrRight ],
              where: self.sequelize.asIs(subQueryJoinOn ? [ subQueryJoinOn ] : [ joinOn ]),
              limit: 1
            }, include.model);

            let subQueryWhere = self.sequelize.asIs([
              '(',
              $query.replace(/;$/, ''),
              ')',
              'IS NOT NULL'
            ].join(' '));

            if (options.where instanceof Utils.and) {
              options.where.args.push(subQueryWhere);
            } else if (_.isPlainObject(options.where)) {
              options.where[ '__' + as ] = subQueryWhere;
            } else {
              options.where = { $and: [ options.where, subQueryWhere ] };
            }
          }

          // Generate join SQL
          joinQueryItem += joinType + self.quoteTable(table, as) + ' ON ' + joinOn;
        }

        if (include.subQuery && subQuery) {
          joinQueries.subQuery.push(joinQueryItem);
        } else {
          joinQueries.mainQuery.push(joinQueryItem);
        }

        if (include.include) {
          include.include.forEach(function (childInclude) {
            if (childInclude._pseudo) return;
            let childJoinQueries = generateJoinQueries(childInclude, as);

            if (childInclude.subQuery && subQuery) {
              joinQueries.subQuery = joinQueries.subQuery.concat(childJoinQueries.subQuery);
            }
            if (childJoinQueries.mainQuery) {
              joinQueries.mainQuery = joinQueries.mainQuery.concat(childJoinQueries.mainQuery);
            }

          }.bind(this));
        }

        return joinQueries;
      };

      // Loop through includes and generate subqueries
      options.include.forEach(function (include) {
        let joinQueries = generateJoinQueries(include, options.tableAs);

        subJoinQueries = subJoinQueries.concat(joinQueries.subQuery);
        mainJoinQueries = mainJoinQueries.concat(joinQueries.mainQuery);

      }.bind(this));
    }

    // If using subQuery select defined subQuery attributes and join subJoinQueries
    if (subQuery) {
      subQueryItems.push('SELECT ' + subQueryAttributes.join(', ') + ' FROM ' + options.table);
      if (mainTableAs) {
        subQueryItems.push(' ' + mainTableAs);
      }
      subQueryItems.push(subJoinQueries.join(''));

      // Else do it the reguar way
    } else {
      mainQueryItems.push('SELECT ' + mainAttributes.join(', ') + ' FROM ' + options.table);
      if (mainTableAs) {
        mainQueryItems.push(' ' + mainTableAs);
      }
      mainQueryItems.push(mainJoinQueries.join(''));
    }

    // Add WHERE to sub or main query
    if (options.hasOwnProperty('where')) {
      options.where = this.getWhereConditions(options.where, mainTableAs || tableName, model, options);
      if (options.where) {
        if (subQuery) {
          subQueryItems.push(' WHERE ' + options.where);
        } else {
          mainQueryItems.push(' WHERE ' + options.where);
        }
      }
    }

    // Add GROUP BY to sub or main query
    if (options.group) {
      options.group = Array.isArray(options.group) ? options.group.map(function (t) {
        return this.quote(t, model);
      }.bind(this)).join(', ') : this.quote(options.group);
      if (subQuery) {
        subQueryItems.push(' GROUP BY ' + options.group);
      } else {
        mainQueryItems.push(' GROUP BY ' + options.group);
      }
    }

    // Add HAVING to sub or main query
    if (options.hasOwnProperty('having')) {
      options.having = this.getWhereConditions(options.having, tableName, model, options, false);
      if (subQuery) {
        subQueryItems.push(' HAVING ' + options.having);
      } else {
        mainQueryItems.push(' HAVING ' + options.having);
      }
    }
    // Add ORDER to sub or main query
    if (options.order) {
      let mainQueryOrder = [];
      let subQueryOrder = [];

      let validateOrder = function (order) {
        if (order instanceof Utils.Literal) return;

        if (!_.includes([
          'ASC',
          'DESC',
          'ASC NULLS LAST',
          'DESC NULLS LAST',
          'ASC NULLS FIRST',
          'DESC NULLS FIRST',
          'NULLS FIRST',
          'NULLS LAST'
        ], order.toUpperCase())) {
          throw new Error(Utils.format('Order must be \'ASC\' or \'DESC\', \'%s\' given', order));
        }
      };

      if (Array.isArray(options.order)) {
        options.order.forEach(function (t) {
          if (Array.isArray(t) && _.size(t) > 1) {
            if (t[ 0 ] instanceof Model || t[ 0 ].model instanceof Model) {
              if (typeof t[ t.length - 2 ] === 'string') {
                validateOrder(_.last(t));
              }
            } else {
              validateOrder(_.last(t));
            }
          }

          if (subQuery && (Array.isArray(t) && !(t[ 0 ] instanceof Model) && !(t[ 0 ].model instanceof Model))) {
            subQueryOrder.push(this.quote(t, model));
          }

          mainQueryOrder.push(this.quote(t, model));
        }.bind(this));
      } else {
        mainQueryOrder.push(this.quote(typeof options.order === 'string' ? new Utils.literal(options.order) : options.order, model));
      }

      if (mainQueryOrder.length) {
        mainQueryItems.push(' ORDER BY ' + mainQueryOrder.join(', '));
      }
      if (subQueryOrder.length) {
        subQueryItems.push(' ORDER BY ' + subQueryOrder.join(', '));
      }
    }

    // Add LIMIT, OFFSET to sub or main query
    // let limitOrder = this.addLimitAndOffset(options, model);
    // if (limitOrder) {
    //   if (subQuery) {
    //     subQueryItems.push(limitOrder);
    //   } else {
    //     mainQueryItems.push(limitOrder);
    //   }
    // }

    // If using subQuery, select attributes from wrapped subQuery and join out join tables
    if (subQuery) {
      query = 'SELECT ' + mainAttributes.join(', ') + ' FROM (';
      query += subQueryItems.join('');
      query += ') ' + options.tableAs;
      query += mainJoinQueries.join('');
      query += mainQueryItems.join('');
    } else {
      query = mainQueryItems.join('');
    }

    // Add LIMIT, OFFSET to sub or main query for Oracle
    query = this.addLimitAndOffset(options, query);

    if (options.lock && this._dialect.supports.lock) {
      let lock = options.lock;
      if (typeof options.lock === 'object') {
        lock = options.lock.level;
      }
      if (this._dialect.supports.lockKey && (lock === 'KEY SHARE' || lock === 'NO KEY UPDATE')) {
        query += ' FOR ' + lock;
      } else if (lock === 'SHARE') {
        query += ' ' + this._dialect.supports.forShare;
      } else {
        query += ' FOR UPDATE';
      }
      if (this._dialect.supports.lockOf && options.lock.of instanceof Model) {
        query += ' OF ' + this.quoteTable(options.lock.of.name);
      }
    }

    // query += ';';

    return query;
    // return {sql:query, bind:{}};
  },

  // OK
  createTableQuery(tableName, attributes, options) {
    //Warning: you must have CREATE ANY TABLE system privilege
    //Warning: you must have CREATE ANY SEQUENCE system privilege
    //Warning: you must have CREATE ANY TRIGGER system privilege
    options = _.extend({}, options || {});


    let query = [
        '-- create table if not exist',
        'DECLARE ',
        '   e_table_exists EXCEPTION; ',
        '   PRAGMA EXCEPTION_INIT(e_table_exists, -00955); ',
        'BEGIN ',
        '',
        '  EXECUTE IMMEDIATE (\'CREATE TABLE <%= table %> (<%= attributes%>) <%= comment %> \'); ',
        '',
        '  <%= sequence %> ',
        '',
        '  <%= trigger %> ',
        '',
        'EXCEPTION ',
        '  WHEN e_table_exists ',
        '    THEN NULL; ',
        'END; '
      ].join(' \n')
      , sequenceTpl = [
        '   -- no drop sequence before if exist',
        '   DECLARE  ',
        '     e_sequence_exists EXCEPTION;  ',
        '     PRAGMA EXCEPTION_INIT(e_sequence_exists, -00955);  ',
        '   BEGIN  ',
        '     EXECUTE IMMEDIATE (\' CREATE  SEQUENCE <%= sequence %> START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE \');  ',
        '',
        '   EXCEPTION ',
        '     WHEN e_sequence_exists ',
        '     THEN NULL; ',
        '   END;'
      ].join(' \n')
      , triggerTpl = [
        '   -- no replace trigger before if exist',
        '   DECLARE  ',
        '     e_trigger_exists EXCEPTION;  ',
        '     PRAGMA EXCEPTION_INIT(e_trigger_exists, -04081);  ',
        '   BEGIN  ',
        '     EXECUTE IMMEDIATE (\' CREATE TRIGGER <%= trigger %>',
        '       BEFORE INSERT ON <%= table %>',
        '       FOR EACH ROW',
        '       ',
        '       BEGIN',
        '         :new.<%= column %> := <%= sequence %>.NEXTVAL;',
        '       END;',
        '     \');',
        '',
        '   EXCEPTION ',
        '     WHEN e_trigger_exists ',
        '     THEN NULL; ',
        '   END;',
      ].join(' \n')
      // let query = [
      //     '-- drop table before if exist',
      //     'BEGIN ' ,
      //     '   DECLARE ' ,
      //     '       e_table_non_exists EXCEPTION; ' ,
      //     '       PRAGMA EXCEPTION_INIT(e_table_non_exists, -00942); ' ,
      //     '   BEGIN ' ,
      //     '       EXECUTE IMMEDIATE (\'DROP TABLE <%= table %> CASCADE CONSTRAINTS\'); ' ,
      //     '   EXCEPTION ' ,
      //     '       WHEN e_table_non_exists ' ,
      //     '       THEN NULL; ' ,
      //     '   END; ' ,
      //     '   EXECUTE IMMEDIATE (\' ' ,
      //     '       CREATE TABLE <%= table %> ( ' ,
      //     '           <%= attributes%> ' ,
      //     '       ) <%= comment %> ' ,
      //     '   \'); ' ,
      //     '',
      //     '<%= sequence %> ',
      //     '',
      //     '<%= trigger %> ',
      //     '',
      //     'END;'
      //   ].join(' \n')
      // , sequenceTpl =[
      //   '   -- drop sequence before if exist',
      //   '   DECLARE  ',
      //   '     e_sequence_non_exists EXCEPTION;  ',
      //   '     PRAGMA EXCEPTION_INIT(e_sequence_non_exists, -02289);  ',
      //   '   BEGIN  ',
      //   '     EXECUTE IMMEDIATE (\'DROP SEQUENCE <%= sequence %> \');  ',
      //   '   EXCEPTION  ',
      //   '     WHEN e_sequence_non_exists  ',
      //   '     THEN NULL;  ',
      //   '   END;  ',
      //   '   EXECUTE IMMEDIATE (\' CREATE  SEQUENCE <%= sequence %> START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE \');  ',
      // ].join(' \n')
      // , triggerTpl = [
      //   '   -- replace trigger if exist',
      //   '   EXECUTE IMMEDIATE (\' CREATE OR REPLACE TRIGGER <%= trigger %>' ,
      //   '   BEFORE INSERT ON <%= table %>' ,
      //   '   FOR EACH ROW' ,
      //   '   ' ,
      //   '   BEGIN' ,
      //   '     :new.<%= column %> := <%= sequence %>.NEXTVAL;' +
      //   '   END;' ,
      //   '   \');'
      // ].join(' \n')
      , primaryKeys = []
      , autoIncrementKeys = []
      , foreignKeys = {}
      , attrStr = []
      , sequences = ''
      , triggers = ''
    ;

    for (let attr in attributes) {
      if (attributes.hasOwnProperty(attr)) {
        let dataType = attributes[ attr ]
          , match;

        if (_.includes(dataType, 'auto_increment')) {
          autoIncrementKeys.push(attr);
          dataType = dataType.replace(/auto_increment/, '');
        }

        if (_.includes(dataType, 'PRIMARY KEY')) {
          primaryKeys.push(attr);
          dataType = dataType.replace(/PRIMARY KEY/, '');
        }

        if (_.includes(dataType, 'REFERENCES')) {
          // MySQL doesn't support inline REFERENCES declarations: move to the end
          match = dataType.match(/^(.+) (REFERENCES.*)$/);
          dataType = match[ 1 ];
          foreignKeys[ attr ] = match[ 2 ];
        }

        attrStr.push(this.quoteIdentifier(attr) + ' ' + dataType);
      }
    }

    if (autoIncrementKeys.length > 0) {
      for (let ikey in autoIncrementKeys) {
        const id = autoIncrementKeys[ ikey ];
        const len = 30 - 5 - id.length;
        let n = tableName;
        if (n.length > len) {
          n = tableName.slice(0, len - 6) + '_' + generateRandomString()
        }

        sequences += '\n\n' + _.template(sequenceTpl)({
          sequence: this.quoteIdentifier(n + '_' + id + '_SEQ')
        }).trim();

        triggers += '\n\n' + _.template(triggerTpl)({
          trigger: this.quoteIdentifier(n + '_' + id + '_TRG'),
          table: this.quoteIdentifier(tableName),
          sequence: this.quoteIdentifier(n + '_' + id + '_SEQ'),
          column: this.quoteIdentifier(id)
        }).trim();
      }
    }

    let values = {
      table: this.quoteTable(tableName),
      attributes: attrStr.join(', ').replace(/'/g, '\'\''),
      comment: options.comment && _.isString(options.comment) ? ' COMMENT ' + this.escape(options.comment) : '',
      sequence: sequences,
      trigger: triggers
      // engine: options.engine,
      // charset: (options.charset ? ' DEFAULT CHARSET=' + options.charset : ''),
      // collation: (options.collate ? ' COLLATE ' + options.collate : ''),
      // initialAutoIncrement: (options.initialAutoIncrement ? ' AUTO_INCREMENT=' + options.initialAutoIncrement : '')
    }
      , pkString = primaryKeys.map(function (pk) {
          return this.quoteIdentifier(pk);
        }.bind(this)).join(', ');

    // if (!!options.uniqueKeys) {
    //   _.each(options.uniqueKeys, function(columns, indexName) {
    //     if (!_.isString(indexName)) {
    //       indexName = 'uniq_' + tableName + '_' + columns.fields.join('_');
    //     }
    //     values.attributes += ', UNIQUE ' + self.quoteIdentifier(indexName) + ' (' + _.map(columns.fields, self.quoteIdentifier).join(', ') + ')';
    //
    //     // values.attributes += ', UNIQUE uniq_' + tableName + '_' + columns.fields.join('_') + ' (' + columns.fields.join(', ') + ')';
    //   });
    // }

    if (pkString.length > 0) {
      values.attributes += ', PRIMARY KEY (' + pkString + ')';
    }

    for (let fkey in foreignKeys) {
      if (foreignKeys.hasOwnProperty(fkey)) {
        values.attributes += ', FOREIGN KEY (' + this.quoteIdentifier(fkey) + ') ' + foreignKeys[ fkey ];
      }
    }

    return _.template(query)(values).trim();
  },

  // Not OK
  describeTableQuery(tableName, schema) {
    let sql = [
      'SELECT',
      'c.COLUMN_NAME AS \'Name\',',
      'c.DATA_TYPE AS \'Type\',',
      'c.CHARACTER_MAXIMUM_LENGTH AS \'Length\',',
      'c.IS_NULLABLE as \'IsNull\',',
      'COLUMN_DEFAULT AS \'Default\',',
      'pk.CONSTRAINT_TYPE AS \'Constraint\'',
      'FROM',
      'INFORMATION_SCHEMA.TABLES t',
      'INNER JOIN',
      'INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA',
      'LEFT JOIN (SELECT tc.table_schema, tc.table_name, ',
      'cu.column_name, tc.constraint_type ',
      'FROM information_schema.TABLE_CONSTRAINTS tc ',
      'JOIN information_schema.KEY_COLUMN_USAGE  cu ',
      'ON tc.table_schema=cu.table_schema and tc.table_name=cu.table_name ',
      'and tc.constraint_name=cu.constraint_name ',
      'and tc.constraint_type=\'PRIMARY KEY\') pk ',
      'ON pk.table_schema=c.table_schema ',
      'AND pk.table_name=c.table_name ',
      'AND pk.column_name=c.column_name ',
      'WHERE t.TABLE_NAME =', wrapSingleQuote(tableName)
    ].join(' ');

    if (schema) {
      sql += 'AND t.TABLE_SCHEMA =' + wrapSingleQuote(schema);
    }

    return sql;
  },

  // Not OK
  renameTableQuery(before, after) {
    const query = 'EXEC sp_rename <%= before %>, <%= after %>;';
    return _.template(query, this._templateSettings)({
      before: this.quoteTable(before),
      after: this.quoteTable(after)
    });
  },

  // Not OK
  showTablesQuery() {
    return 'SELECT table_name FROM user_tables';
  },

  // OK
  dropTableQuery(tableName, options) {
    options = options || {};
    let query = [
      'DECLARE ',
      '    e_table_non_exists EXCEPTION; ',
      '    PRAGMA EXCEPTION_INIT(e_table_non_exists, -00942); ',
      'BEGIN ',
      '    EXECUTE IMMEDIATE (\'DROP TABLE <%= table %> <%= cascade %>\'); ',
      'EXCEPTION ',
      '    WHEN e_table_non_exists ',
      '    THEN NULL; ',
      'END;'
    ].join(' \n');

    return _.template(query)({
      table: this.quoteTable(tableName),
      cascade: options.cascade ? ' CASCADE CONSTRAINTS' : ''
    });
  },

  // OK
  addColumnQuery(table, key, dataType) {
    // FIXME: attributeToSQL SHOULD be using attributes in addColumnQuery
    //        but instead we need to pass the key along as the field here
    dataType.field = key;

    const query = 'ALTER TABLE <%= table %> ADD <%= attribute %>',
      attribute = _.template('<%= key %> <%= definition %>', this._templateSettings)({
        key: this.quoteIdentifier(key),
        definition: this.attributeToSQL(dataType, key)
      });

    return _.template(query, this._templateSettings)({
      table: this.quoteTable(table),
      attribute
    });
  },

  // OK
  removeColumnQuery(tableName, attributeName) {
    const query = 'ALTER TABLE <%= tableName %> DROP COLUMN <%= attributeName %>';
    return _.template(query, this._templateSettings)({
      tableName: this.quoteTable(tableName),
      attributeName: this.quoteIdentifier(attributeName)
    });
  },

  // OK
  changeColumnQuery(tableName, attributes) {
    const query = 'ALTER TABLE <%= tableName %> <%= query %>';
    const attrString = [],
      constraintString = [];

    for (const attributeName in attributes) {
      const definition = attributes[ attributeName ];
      if (definition.match(/REFERENCES/)) {
        constraintString.push(_.template('<%= fkName %> FOREIGN KEY (<%= attrName %>) <%= definition %>', this._templateSettings)({
          fkName: this.quoteIdentifier(attributeName + '_foreign_idx'),
          attrName: this.quoteIdentifier(attributeName),
          definition: definition.replace(/.+?(?=REFERENCES)/, '')
        }));
      } else {
        attrString.push(_.template('<%= attrName %> <%= definition %>', this._templateSettings)({
          attrName: this.quoteIdentifier(attributeName),
          definition
        }));
      }
    }

    let finalQuery = '';
    if (attrString.length) {
      finalQuery += 'MODIFY ' + attrString.join(', ');
      finalQuery += constraintString.length ? ' ' : '';
    }
    if (constraintString.length) {
      finalQuery += 'ADD CONSTRAINT ' + constraintString.join(', ');
    }

    return _.template(query, this._templateSettings)({
      tableName: this.quoteTable(tableName),
      query: finalQuery
    });
  },

  // Not OK
  renameColumnQuery(tableName, attrBefore, attributes) {
    const query = 'ALTER TABLE <%= tableName %> RENAME COLUMN <%= before %> TO <%= after %>';

    return _.template(query, this._templateSettings)({
      tableName: this.quoteTable(tableName),
      before: this.quoteIdentifier(attrBefore),
      after: this.quoteIdentifier(attributes)
    });
  },

  insertQuery: function (table, valueHash, modelAttributes, options) {
    options = options || {};
    // let self=this;

    let query
      , valueQuery = 'INSERT<%= ignore %> INTO <%= table %> (<%= attributes %>)<%= output %> VALUES (<%= values %>)'
      , emptyQuery = 'INSERT<%= ignore %> INTO <%= table %><%= output %>'
      , outputFragment
      , fields = []
      , values = []
      , key
      , value
      , identityWrapperRequired = false
      , modelAttributeMap = {}
      , bindParameters = {}
    ;

    if (modelAttributes) {
      _.each(modelAttributes, function (attribute, key) {
        modelAttributeMap[ key ] = attribute;
        if (attribute.field) {
          modelAttributeMap[ attribute.field ] = attribute;
        }
      });
    }

    if (this._dialect.supports[ 'DEFAULT VALUES' ]) {
      emptyQuery += ' DEFAULT VALUES';
    } else if (this._dialect.supports[ 'VALUES ()' ]) {
      emptyQuery += ' VALUES ()';
    }

    if (this._dialect.supports.EXCEPTION && options.exception) {
      // Mostly for internal use, so we expect the user to know what he's doing!
      // pg_temp functions are private per connection, so we never risk this function interfering with another one.

      // <= 9.1
      //options.exception = 'WHEN unique_violation THEN NULL;';
      //valueQuery = 'CREATE OR REPLACE FUNCTION pg_temp.testfunc() RETURNS SETOF <%= table %> AS $body$ BEGIN RETURN QUERY ' + valueQuery + '; EXCEPTION ' + options.exception + ' END; $body$ LANGUAGE plpgsql; SELECT * FROM pg_temp.testfunc(); DROP FUNCTION IF EXISTS pg_temp.testfunc();';

      // >= 9.2 - Use a UUID but prefix with 'func_' (numbers first not allowed)
      let delimiter = '$func_' + Utils.generateUUID().replace(/-/g, '') + '$';

      options.exception = 'WHEN unique_violation THEN GET STACKED DIAGNOSTICS sequelize_caught_exception = PG_EXCEPTION_DETAIL;';
      valueQuery = 'CREATE OR REPLACE FUNCTION pg_temp.testfunc(OUT response <%= table %>, OUT sequelize_caught_exception text) RETURNS RECORD AS ' + delimiter +
        ' BEGIN ' + valueQuery + ' INTO response; EXCEPTION ' + options.exception + ' END ' + delimiter +
        ' LANGUAGE plpgsql; SELECT (testfunc.response).*, testfunc.sequelize_caught_exception FROM pg_temp.testfunc(); DROP FUNCTION IF EXISTS pg_temp.testfunc()';
    }

    if (this._dialect.supports[ 'ON DUPLICATE KEY' ] && options.onDuplicate) {
      valueQuery += ' ON DUPLICATE KEY ' + options.onDuplicate;
      emptyQuery += ' ON DUPLICATE KEY ' + options.onDuplicate;
    }

    valueHash = Utils.removeNullValuesFromHash(valueHash, this.options.omitNull);

    for (key in valueHash) {
      if (valueHash.hasOwnProperty(key)) {
        value = valueHash[ key ];
        fields.push(this.quoteIdentifier(key));

        // SERIALS' can't be NULL in postgresql, use DEFAULT where supported
        if (modelAttributeMap && modelAttributeMap[ key ] && modelAttributeMap[ key ].autoIncrement === true && !value) {
          if (!this._dialect.supports.autoIncrement.defaultValue) {
            fields.splice(-1, 1);
          } else if (this._dialect.supports.DEFAULT) {
            values.push('DEFAULT');
          } else {
            values.push(this.escape(null));
          }


          if (this._dialect.supports.returnValues && options.returning) {
            if (!!this._dialect.supports.returnValues.returning) {
              valueQuery += ' RETURNING ' + this.quoteIdentifier(key) + ' INTO $rid';
              emptyQuery += ' RETURNING ' + this.quoteIdentifier(key) + ' INTO $rid';
              bindParameters = {
                rid: {
                  type: this.sequelize.connectionManager.lib.NUMBER,
                  dir: this.sequelize.connectionManager.lib.BIND_OUT
                }
              };

            } else if (!!this._dialect.supports.returnValues.output) {
              outputFragment = ' OUTPUT INSERTED.*';
            }
          }
        } else {
          if (modelAttributeMap && modelAttributeMap[ key ] && modelAttributeMap[ key ].autoIncrement === true) {
            identityWrapperRequired = true;
          }

          if (typeof value === 'string' && value.length >= 4000) {
            // Oracle don't support if length of string literal greater than 4000 chars.
            values.push('$' + key);
            bindParameters[key] = {
              type: this.sequelize.connectionManager.lib.CLOB,
              dir: this.sequelize.connectionManager.lib.BIND_IN,
              value,
            }
          } else {
            values.push(this.escape(value, (modelAttributeMap && modelAttributeMap[ key ]) || undefined));
          }
        }
      }
    }

    let replacements = {
      ignore: options.ignore ? this._dialect.supports.IGNORE : '',
      table: this.quoteTable(table),
      attributes: fields.join(','),
      output: outputFragment,
      values: values.join(',')
    };

    query = replacements.attributes.length ? valueQuery : emptyQuery;
    if (identityWrapperRequired && this._dialect.supports.autoIncrement.identityInsert) {
      query = [
        'SET IDENTITY_INSERT', this.quoteTable(table), 'ON;',
        query,
        'SET IDENTITY_INSERT', this.quoteTable(table), 'OFF;',
      ].join(' ');
    }

    return {
      query: _.template(query)(replacements),
      bind: bindParameters
    };
  },

  // Not OK
  bulkInsertQuery(tableName, attrValueHashes) {
    const query = 'INSERT ALL <%= tuples %> SELECT * FROM dual';
    const attributesTpl = 'INTO <%= table %> (<%= columns %>)'
      , tuples = []
      , allAttributes = []
    ;

    _.forEach(attrValueHashes, function(attrValueHash, i) {
      _.forOwn(attrValueHash, function(value, key, hash) {
        if (allAttributes.indexOf(key) === -1){ allAttributes.push(key); }
      });
    });

    const attributes = _.template(attributesTpl)({
      table: this.quoteIdentifier(tableName),
      columns: allAttributes.map(function (attr) {
        return this.quoteIdentifier(attr);
      }.bind(this)).join(',')
    });

    const bindParameters = {};
    let paramIndex = 0;
    _.forEach(attrValueHashes, function(attrValueHash, i) {
      tuples.push(attributes + ' VALUES (' +
        allAttributes.map(function (key) {
          const value = attrValueHash[key];
          if (typeof value === 'string' && value.length >= 4000) {
            paramIndex++;
            const paramName = "param__" + paramIndex;
            bindParameters[paramName] = {
              type: this.sequelize.connectionManager.lib.CLOB,
              dir: this.sequelize.connectionManager.lib.BIND_IN,
              value,
            };
            return "$" + paramName;
          } else {
            return this.escape(value);
          }
        }.bind(this)).join(',') +
        ')');
    }.bind(this));

    var replacements  = {
      // ignoreDuplicates: options && options.ignoreDuplicates ? ' IGNORE' : '',
      // table: this.quoteIdentifier(tableName),
      // attributes: ,
      tuples: tuples.join(' ')
    };

    if (paramIndex > 0) {
      return {
        query: _.template(query)(replacements),
        bind: bindParameters,
      }
    }

    return _.template(query)(replacements);
  },

  // OK
  updateQuery(tableName, attrValueHash, where, options, attributes) {
    if (options.limit) {
      throw new Error('Limit is not support by update method for now.');
    }

    let sql = super.updateQuery(tableName, attrValueHash, where, options, attributes);
    return {
      query: sql + ' RETURNING COUNT(*) INTO $affectedRows',
      bind: {
        affectedRows: {
          type: this.sequelize.connectionManager.lib.NUMBER,
          dir: this.sequelize.connectionManager.lib.BIND_OUT
        }
      }
    };
  },

  // Not OK
  upsertQuery(tableName, insertValues, updateValues, where, model) {
    const targetTableAlias = this.quoteTable(`${tableName}_target`);
    const sourceTableAlias = this.quoteTable(`${tableName}_source`);
    const primaryKeysAttrs = [];
    const identityAttrs = [];
    const uniqueAttrs = [];
    const tableNameQuoted = this.quoteTable(tableName);
    let needIdentityInsertWrapper = false;


    //Obtain primaryKeys, uniquekeys and identity attrs from rawAttributes as model is not passed
    for (const key in model.rawAttributes) {
      if (model.rawAttributes[ key ].primaryKey) {
        primaryKeysAttrs.push(model.rawAttributes[ key ].field || key);
      }
      if (model.rawAttributes[ key ].unique) {
        uniqueAttrs.push(model.rawAttributes[ key ].field || key);
      }
      if (model.rawAttributes[ key ].autoIncrement) {
        identityAttrs.push(model.rawAttributes[ key ].field || key);
      }
    }

    //Add unique indexes defined by indexes option to uniqueAttrs
    for (const index of model.options.indexes) {
      if (index.unique && index.fields) {
        for (const field of index.fields) {
          const fieldName = typeof field === 'string' ? field : field.name || field.attribute;
          if (uniqueAttrs.indexOf(fieldName) === -1 && model.rawAttributes[ fieldName ]) {
            uniqueAttrs.push(fieldName);
          }
        }
      }
    }

    const updateKeys = Object.keys(updateValues);
    const insertKeys = Object.keys(insertValues);
    const insertKeysQuoted = insertKeys.map(key => this.quoteIdentifier(key)).join(', ');
    const insertValuesEscaped = insertKeys.map(key => this.escape(insertValues[ key ])).join(', ');
    const sourceTableQuery = `VALUES(${insertValuesEscaped})`; //Virtual Table
    let joinCondition;

    //IDENTITY_INSERT Condition
    identityAttrs.forEach(key => {
      if (updateValues[ key ] && updateValues[ key ] !== null) {
        needIdentityInsertWrapper = true;
        /*
         * IDENTITY_INSERT Column Cannot be updated, only inserted
         * http://stackoverflow.com/a/30176254/2254360
         */
      }
    });

    //Filter NULL Clauses
    const clauses = where[ Op.or ].filter(clause => {
      let valid = true;
      /*
       * Exclude NULL Composite PK/UK. Partial Composite clauses should also be excluded as it doesn't guarantee a single row
       */
      for (const key in clause) {
        if (!clause[ key ]) {
          valid = false;
          break;
        }
      }
      return valid;
    });

    /*
     * Generate ON condition using PK(s).
     * If not, generate using UK(s). Else throw error
     */
    const getJoinSnippet = array => {
      return array.map(key => {
        key = this.quoteIdentifier(key);
        return `${targetTableAlias}.${key} = ${sourceTableAlias}.${key}`;
      });
    };

    if (clauses.length === 0) {
      throw new Error('Primary Key or Unique key should be passed to upsert query');
    } else {
      // Search for primary key attribute in clauses -- Model can have two separate unique keys
      for (const key in clauses) {
        const keys = Object.keys(clauses[ key ]);
        if (primaryKeysAttrs.indexOf(keys[ 0 ]) !== -1) {
          joinCondition = getJoinSnippet(primaryKeysAttrs).join(' AND ');
          break;
        }
      }
      if (!joinCondition) {
        joinCondition = getJoinSnippet(uniqueAttrs).join(' AND ');
      }
    }

    // Remove the IDENTITY_INSERT Column from update
    const updateSnippet = updateKeys.filter(key => {
      if (identityAttrs.indexOf(key) === -1) {
        return true;
      } else {
        return false;
      }
    })
      .map(key => {
        const value = this.escape(updateValues[ key ]);
        key = this.quoteIdentifier(key);
        return `${targetTableAlias}.${key} = ${value}`;
      }).join(', ');

    const insertSnippet = `(${insertKeysQuoted}) VALUES(${insertValuesEscaped})`;
    let query = `MERGE INTO ${tableNameQuoted} WITH(HOLDLOCK) AS ${targetTableAlias} USING (${sourceTableQuery}) AS ${sourceTableAlias}(${insertKeysQuoted}) ON ${joinCondition}`;
    query += ` WHEN MATCHED THEN UPDATE SET ${updateSnippet} WHEN NOT MATCHED THEN INSERT ${insertSnippet} OUTPUT $action, INSERTED.*;`;
    if (needIdentityInsertWrapper) {
      query = `SET IDENTITY_INSERT ${tableNameQuoted} ON; ${query} SET IDENTITY_INSERT ${tableNameQuoted} OFF;`;
    }
    return query;
  },

  // Not OK
  deleteQuery(tableName, where, options) {
    options = options || {};

    const table = this.quoteTable(tableName);
    if (options.truncate === true) {
      // Truncate does not allow LIMIT and WHERE
      return 'TRUNCATE TABLE ' + table;
    }

    where = this.getWhereConditions(where);

    // TODO: Very strange design.
    // if (_.isUndefined(options.limit)) {
    //   options.limit = 1;
    // }

    if (options.limit) {
      throw new Error('Not support now!');
    } else {
      const query = 'DELETE FROM <%= table %><%= where %>' +
        'RETURNING COUNT(*) INTO $affectedRows';
      const replacements = {
        table,
        where
      };

      if (replacements.where) {
        replacements.where = ' WHERE ' + replacements.where;
      }

      return {
        query: _.template(query, this._templateSettings)(replacements),
        bind: {
          affectedRows: {
            type: this.sequelize.connectionManager.lib.NUMBER,
            dir: this.sequelize.connectionManager.lib.BIND_OUT
          }
        }
      };
    }
  },

  // OK
  showIndexesQuery(tableName) {
    if (!_.isString(tableName)) {
      tableName = tableName.tableName;
    }

    // tableName = this.quoteTable(tableName);

    let query = 'SELECT index_name FROM user_indexes ' +
      'WHERE table_name = \'<%= tableName %>\'';

    return _.template(query)({ tableName: tableName });
  },

  // Not OK
  showConstraintsQuery(tableName) {
    return `EXEC sp_helpconstraint @objname = ${this.escape(this.quoteTable(tableName))};`;
  },

  // Not OK
  removeIndexQuery(tableName, indexNameOrAttributes) {
    const sql = 'DROP INDEX <%= indexName %> ON <%= tableName %>';
    let indexName = indexNameOrAttributes;

    if (typeof indexName !== 'string') {
      indexName = Utils.underscore(tableName + '_' + indexNameOrAttributes.join('_'));
    }

    const values = {
      tableName: this.quoteIdentifiers(tableName),
      indexName: this.quoteIdentifiers(indexName)
    };

    return _.template(sql, this._templateSettings)(values);
  },

  // OK
  attributeToSQL(attribute, name) {
    if (!_.isPlainObject(attribute)) {
      attribute = {
        type: attribute
      };
    }

    let template;

    if (attribute.type instanceof DataTypes.ENUM) {
      let len = 1;
      for (let i in attribute.type.values) {
        if (len < attribute.type.values[ i ].length) {
          len = attribute.type.values[ i ].length;
        }
      }
      if (Array.isArray(attribute.type.values) && (attribute.type.values.length > 0)) {
        template = 'VARCHAR2(' + len + ') CHECK( "' + name + '" IN (' + _.map(attribute.type.values, function (value) {
          return this.escape(value);
        }.bind(this)).join(', ') + '))';
      } else {
        throw new Error('Values for ENUM haven\'t been defined.');
      }
      // if (attribute.type.values && !attribute.values) attribute.values = attribute.type.values;
      // template = 'ENUM(' + _.map(attribute.values, function(value) {
      //   return this.escape(value);
      // }.bind(this)).join(', ') + ')';
    } else {
      template = attribute.type.toString();
    }

    if (attribute.allowNull === false) {
      template += ' NOT NULL';
    }

    if (attribute.autoIncrement) {
      template += ' auto_increment';
    }

    // Blobs/texts cannot have a defaultValue
    //in oracle, we connot have NOT NULL AND DEFAULT in the same time
    if (attribute.allowNull !== false && attribute.type !== 'TEXT' && attribute.type._binary !== true && Utils.defaultValueSchemable(attribute.defaultValue)) {
      template += ' DEFAULT ' + this.escape(attribute.defaultValue);
    }

    if (attribute.unique === true) {
      template += ' UNIQUE';
    }

    if (attribute.primaryKey) {
      template += ' PRIMARY KEY';
    }

    if (attribute.references) {
      template += ' REFERENCES ' + this.quoteTable(attribute.references.model);

      if (attribute.references.key) {
        template += ' (' + this.quoteIdentifier(attribute.references.key) + ')';
      } else {
        template += ' (' + this.quoteIdentifier('id') + ')';
      }

      if (attribute.onDelete) {
        template += ' ON DELETE ' + attribute.onDelete.toUpperCase();
      }

      //Oracle no support
      // if (attribute.onUpdate) {
      //   template += ' ON UPDATE ' + attribute.onUpdate.toUpperCase();
      // }
    }

    return template;
  },

  // OK
  attributesToSQL(attributes, options) {
    const result = {};

    for (const key in attributes) {
      const attribute = attributes[ key ];
      result[ attribute.field || key ] = this.attributeToSQL(attribute, key);
    }

    return result;
  },

  // Not OK
  createTrigger() {
    throwMethodUndefined('createTrigger');
  },

  // Not OK
  dropTrigger() {
    throwMethodUndefined('dropTrigger');
  },

  // Not OK
  renameTrigger() {
    throwMethodUndefined('renameTrigger');
  },

  // Not OK
  createFunction() {
    throwMethodUndefined('createFunction');
  },

  // Not OK
  dropFunction() {
    throwMethodUndefined('dropFunction');
  },

  // Not OK
  renameFunction() {
    throwMethodUndefined('renameFunction');
  },

  // OK
  quoteIdentifier(identifier) {
    if (identifier === '*') return identifier;
    return Utils.addTicks(Utils.removeTicks(identifier, '"'), '"');
  },

  // OK
  quoteTable(param, as) {
    let table = '';

    if (as === true) {
      as = param.as || param.name || param;
    }

    if (_.isObject(param)) {
      if (this._dialect.supports.schemas) {
        if (param.schema) {
          table += this.quoteIdentifier(param.schema) + '.';
        }

        table += this.quoteIdentifier(param.tableName);
      } else {
        if (param.schema) {
          table += param.schema + (param.delimiter || '.');
        }

        table += param.tableName;
        table = this.quoteIdentifier(table);
      }


    } else {
      table = this.quoteIdentifier(param);
    }

    if (as) {
      table += ' ' + this.quoteIdentifier(as);
    }
    return table;
  },

  // Not OK
  getForeignKeysQuery(table) {
    const tableName = table.tableName || table;
    let sql = [
      'SELECT',
      'constraint_name = C.CONSTRAINT_NAME',
      'FROM',
      'INFORMATION_SCHEMA.TABLE_CONSTRAINTS C',
      'WHERE C.CONSTRAINT_TYPE = \'FOREIGN KEY\'',
      'AND C.TABLE_NAME =', wrapSingleQuote(tableName)
    ].join(' ');

    if (table.schema) {
      sql += ' AND C.TABLE_SCHEMA =' + wrapSingleQuote(table.schema);
    }

    return sql;
  },

  // Not OK
  getForeignKeyQuery(table, attributeName) {
    const tableName = table.tableName || table;
    let sql = [
      'SELECT',
      'constraint_name = TC.CONSTRAINT_NAME',
      'FROM',
      'INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC',
      'JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE CCU',
      'ON TC.CONSTRAINT_NAME = CCU.CONSTRAINT_NAME',
      'WHERE TC.CONSTRAINT_TYPE = \'FOREIGN KEY\'',
      'AND TC.TABLE_NAME =', wrapSingleQuote(tableName),
      'AND CCU.COLUMN_NAME =', wrapSingleQuote(attributeName)
    ].join(' ');

    if (table.schema) {
      sql += ' AND TC.TABLE_SCHEMA =' + wrapSingleQuote(table.schema);
    }

    return sql;
  },

  // Not OK
  getPrimaryKeyConstraintQuery(table, attributeName) {
    const tableName = wrapSingleQuote(table.tableName || table);
    return [
      'SELECT K.TABLE_NAME AS tableName,',
      'K.COLUMN_NAME AS columnName,',
      'K.CONSTRAINT_NAME AS constraintName',
      'FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS C',
      'JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS K',
      'ON C.TABLE_NAME = K.TABLE_NAME',
      'AND C.CONSTRAINT_CATALOG = K.CONSTRAINT_CATALOG',
      'AND C.CONSTRAINT_SCHEMA = K.CONSTRAINT_SCHEMA',
      'AND C.CONSTRAINT_NAME = K.CONSTRAINT_NAME',
      'WHERE C.CONSTRAINT_TYPE = \'PRIMARY KEY\'',
      `AND K.COLUMN_NAME = ${wrapSingleQuote(attributeName)}`,
      `AND K.TABLE_NAME = ${tableName};`
    ].join(' ');
  },

  // Not OK
  dropForeignKeyQuery(tableName, foreignKey) {
    return _.template('ALTER TABLE <%= table %> DROP <%= key %>', this._templateSettings)({
      table: this.quoteTable(tableName),
      key: this.quoteIdentifier(foreignKey)
    });
  },

  // Not OK
  getDefaultConstraintQuery(tableName, attributeName) {
    const sql = 'SELECT name FROM SYS.DEFAULT_CONSTRAINTS ' +
      'WHERE PARENT_OBJECT_ID = OBJECT_ID(\'<%= table %>\', \'U\') ' +
      'AND PARENT_COLUMN_ID = (SELECT column_id FROM sys.columns WHERE NAME = (\'<%= column %>\') ' +
      'AND object_id = OBJECT_ID(\'<%= table %>\', \'U\'));';
    return _.template(sql, this._templateSettings)({
      table: this.quoteTable(tableName),
      column: attributeName
    });
  },

  // Not OK
  dropConstraintQuery(tableName, constraintName) {
    const sql = 'ALTER TABLE <%= table %> DROP CONSTRAINT <%= constraint %>;';
    return _.template(sql, this._templateSettings)({
      table: this.quoteTable(tableName),
      constraint: this.quoteIdentifier(constraintName)
    });
  },

  // Not OK
  setAutocommitQuery() {
    return '';
  },

  // Not OK
  setIsolationLevelQuery() {

  },

  // Not OK
  generateTransactionId() {
    return randomBytes(10).toString('hex');
  },

  // Not OK
  startTransactionQuery(transaction) {
    if (transaction.parent) {
      return 'SAVE TRANSACTION ' + this.quoteIdentifier(transaction.name) + ';';
    }

    return 'BEGIN TRANSACTION;';
  },

  // Not OK
  commitTransactionQuery(transaction) {
    if (transaction.parent) {
      return;
    }

    return 'COMMIT TRANSACTION;';
  },

  // Not OK
  rollbackTransactionQuery(transaction) {
    if (transaction.parent) {
      return 'ROLLBACK TRANSACTION ' + this.quoteIdentifier(transaction.name) + ';';
    }

    return 'ROLLBACK TRANSACTION;';
  },

  // Not OK
  selectFromTableFragment(options, model, attributes, tables, mainTableAs, where) {
    let topFragment = '';
    let mainFragment = 'SELECT ' + attributes.join(', ') + ' FROM ' + tables;

    // Handle SQL Server 2008 with TOP instead of LIMIT
    if (semver.valid(this.sequelize.options.databaseVersion) && semver.lt(this.sequelize.options.databaseVersion, '11.0.0')) {
      if (options.limit) {
        topFragment = 'TOP ' + options.limit + ' ';
      }
      if (options.offset) {
        const offset = options.offset || 0,
          isSubQuery = options.hasIncludeWhere || options.hasIncludeRequired || options.hasMultiAssociation;
        let orders = { mainQueryOrder: [] };
        if (options.order) {
          orders = this.getQueryOrders(options, model, isSubQuery);
        }

        if (!orders.mainQueryOrder.length) {
          orders.mainQueryOrder.push(this.quoteIdentifier(model.primaryKeyField));
        }

        const tmpTable = mainTableAs ? mainTableAs : 'OffsetTable';
        const whereFragment = where ? ' WHERE ' + where : '';

        /*
         * For earlier versions of SQL server, we need to nest several queries
         * in order to emulate the OFFSET behavior.
         *
         * 1. The outermost query selects all items from the inner query block.
         *    This is due to a limitation in SQL server with the use of computed
         *    columns (e.g. SELECT ROW_NUMBER()...AS x) in WHERE clauses.
         * 2. The next query handles the LIMIT and OFFSET behavior by getting
         *    the TOP N rows of the query where the row number is > OFFSET
         * 3. The innermost query is the actual set we want information from
         */
        const fragment = 'SELECT TOP 100 PERCENT ' + attributes.join(', ') + ' FROM ' +
          '(SELECT ' + topFragment + '*' +
          ' FROM (SELECT ROW_NUMBER() OVER (ORDER BY ' + orders.mainQueryOrder.join(', ') + ') as row_num, * ' +
          ' FROM ' + tables + ' AS ' + tmpTable + whereFragment + ')' +
          ' AS ' + tmpTable + ' WHERE row_num > ' + offset + ')' +
          ' AS ' + tmpTable;
        return fragment;
      } else {
        mainFragment = 'SELECT ' + topFragment + attributes.join(', ') + ' FROM ' + tables;
      }
    }

    if (mainTableAs) {
      mainFragment += ' AS ' + mainTableAs;
    }

    return mainFragment;
  },

  // Not OK
  addLimitAndOffset(options, query) {
    query = query || '';
    if (!options.offset && options.limit) {
      query = ' SELECT * FROM (  SELECT t.*, ROWNUM ROWNUM_1 FROM (' + query + ')t )t2 WHERE t2.ROWNUM_1 <=' + options.limit;
    }

    if (options.offset && !options.limit) {
      query = ' SELECT * FROM (  SELECT t.*, ROWNUM ROWNUM_1 FROM (' + query + ')t )t2 WHERE t2.ROWNUM_1 >' + options.offset;
    }
    if (options.offset && options.limit) {
      query = ' SELECT * FROM (  SELECT t.*, ROWNUM ROWNUM_1 FROM (' + query + ')t )t2 WHERE t2.ROWNUM_1 BETWEEN ' + (parseInt(options.offset, 10) + 1) + ' AND ' + (parseInt(options.offset, 10) + parseInt(options.limit, 10));
    }
    return query;
  },

  // Not OK
  booleanValue(value) {
    return value ? 1 : 0;
  },

  escape(value, field) {
    if (value && value._isSequelizeMethod) {
      return this.handleSequelizeMethod(value);
    } else {
      return SqlString.escape(value, false, this.options.timezone, this.dialect, field);
    }
  }
};

// private methods
function wrapSingleQuote(identifier) {
  return Utils.addTicks(Utils.removeTicks(identifier, '\''), '\'');
}

module.exports = QueryGenerator;
