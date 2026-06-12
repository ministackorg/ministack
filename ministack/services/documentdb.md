

Prompts

This codebase is Ministack, it mimicks AWS services locally for testing. I want to add the AWS DocumentDB(DocDB) service based on the @CONTRIBUTING.md guide. The file that will contain the implementation is @ministack/services/documentdb.py , I need to implement the available API endpoint that DocDB supports. 
For the moment, following the contributing guide, implement just the aggregation, authentication, diagnostic, query and write operations and role management comamnds sections in @ministack/services/documentdb-apis.md , we'll the rest later. You can use the @ministack/services/rds.py as an example but careful, the file is very long.

This codebase is Ministack, it mimicks AWS services locally for testing. I want to add the AWS DocumentDB(DocDB) service based on the @CONTRIBUTING.md guide. The file that will contain the implementation is @ministack/services/documentdb.py , I need to implement the available API endpoint that DocDB supports. 
For the moment, following the contributing guide, implement just the Sessions, User, Sharding, Array, Bitwise and Comment sections in @ministack/services/documentdb-apis.md , we'll the rest later. You can use the @ministack/services/rds.py as an example but careful, the file is very long.


Implemented Administrative cmds, aggregation, authentication, diagnostic, query and write operations and role management cmds.



# Supported MongoDB APIs, operations, and data types in Amazon DocumentDB
<a name="mongo-apis"></a>

Amazon DocumentDB (with MongoDB compatibility) is a fast, scalable, highly-available, and fully managed document database service that supports MongoDB workloads. Amazon DocumentDB is compatible with the MongoDB 3.6, 4.0, 5.0, and 8.0 APIs. This section lists the supported functionality. For support using MongoDB APIs and drivers, please consult the MongoDB Community Forums. For support using the Amazon DocumentDB service, please contact the appropriate AWS support team. For functional differences between Amazon DocumentDB and MongoDB, please see [Functional differences: Amazon DocumentDB and MongoDB](functional-differences.md). 

MongoDB commands and operators that are internal-only or not applicable to a fully-managed service are not supported and are not included in the list of supported functionality.

We have added over 50\+ additional capabilities since launch, and will continue to work backwards from our customers to deliver the capabilities that they need. For information on the most recent launches, see [Amazon DocumentDB Announcements](https://aws.amazon.com/documentdb/resources/).

If there is a feature that isn't supported that you'd like us to build, let us know by sending an email with your accountID, the requested features, and use case to the [Amazon DocumentDB service team](mailto:documentdb-feature-request@amazon.com).
+ [Database commands](#mongo-apis-database)
+ [Query and projection operators](#mongo-apis-query)
+ [Update operators](#mongo-apis-update)
+ [Geospatial](#mongo-apis-geospatial)
+ [Cursor methods](#mongo-apis-cursor)
+ [Aggregation pipeline operators](#mongo-apis-aggregation-pipeline)
+ [Data types](#mongo-apis-data-types)
+ [Indexes](#mongo-apis-indexes)

## Database commands
<a name="mongo-apis-database"></a>

**Topics**
+ [Administrative Commands](#mongo-apis-dababase-administrative)
+ [Aggregation](#mongo-apis-dababase-aggregation)
+ [Authentication](#mongo-apis-dababase-authentication)
+ [Diagnostic commands](#mongo-apis-dababase-diagnostics)
+ [Query and write operations](#mongo-apis-dababase-query-write)
+ [Role management commands](#mongo-apis-database-role-management)
+ [Sessions commands](#mongo-apis-dababase-sessions)
+ [User management](#mongo-apis-dababase-user-management)
+ [Sharding commands](#mongo-apis-dababase-sharding)

### Administrative Commands
<a name="mongo-apis-dababase-administrative"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| Capped Collections | No | No | No | No | No | 
| cloneCollectionAsCapped | No | No | No | No | No | 
| collMod | Partial | Partial | Partial | Partial | Partial | 
| collMod: expireAfterSeconds | Yes | Yes | Yes | Yes | Yes | 
| convertToCapped | No | No | No | No | No | 
| copydb | No | No | No | No | No | 
| create | Yes | Yes | Yes | Yes | Yes | 
| createView | No | No | No | Yes | No | 
| createIndexes | Yes | Yes | Yes | Yes | Yes | 
| currentOp | Yes | Yes | Yes | Yes | Yes | 
| drop | Yes | Yes | Yes | Yes | Yes | 
| dropDatabase | Yes | Yes | Yes | Yes | Yes | 
| dropIndexes | Yes | Yes | Yes | Yes | Yes | 
| filemd5 | No | No | No | No | No | 
| getAuditConfig | No | Yes | Yes | Yes | No | 
| killCursors | Yes | Yes | Yes | Yes | Yes | 
| killOp | Yes | Yes | Yes | Yes | Yes | 
| listCollections\* | Yes | Yes | Yes | Yes | Yes | 
| listDatabases | Yes | Yes | Yes | Yes | Yes | 
| listIndexes | Yes | Yes | Yes | Yes | Yes | 
| reIndex | No | No | Yes | Yes | No | 
| renameCollection | Yes | Yes | Yes | Yes | No | 
| setAuditConfig | No | Yes | Yes | Yes | No | 

\* The `type` key in the filter option is not supported.

### Aggregation
<a name="mongo-apis-dababase-aggregation"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| aggregate | Yes | Yes | Yes | Yes | Yes | 
| count | Yes | Yes | Yes | Yes | Yes | 
| distinct | Yes | Yes | Yes | Yes | Yes | 
| mapReduce | No | No | No | Yes | No | 

### Authentication
<a name="mongo-apis-dababase-authentication"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| authenticate | Yes | Yes | Yes | Yes | Yes | 
| logout | Yes | Yes | Yes | Yes | Yes | 

### Diagnostic commands
<a name="mongo-apis-dababase-diagnostics"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| buildInfo | Yes | Yes | Yes | Yes | Yes | 
| collStats | Yes | Yes | Yes | Yes | Yes | 
| connPoolStats | No | No | No | No | No | 
| connectionStatus | Yes | Yes | Yes | Yes | Yes | 
| dataSize | Yes | Yes | Yes | Yes | Yes | 
| dbHash | No | No | No | No | No | 
| dbStats | Yes | Yes | Yes | Yes | Yes | 
| explain | Yes | Yes | Yes | Yes | Yes | 
| explain: executionStats | Yes | Yes | Yes | Yes | Yes | 
| features | No | No | No | No | No | 
| hostInfo | Yes | Yes | Yes | Yes | Yes | 
| listCommands | Yes | Yes | Yes | Yes | Yes | 
| profiler | [Yes](https://docs.aws.amazon.com//documentdb/latest/developerguide/profiling.html) | [Yes](https://docs.aws.amazon.com//documentdb/latest/developerguide/profiling.html) | [Yes](https://docs.aws.amazon.com//documentdb/latest/developerguide/profiling.html) | [Yes](https://docs.aws.amazon.com//documentdb/latest/developerguide/profiling.html) | No | 
| serverStatus | Yes | Yes | Yes | Yes | Yes | 
| top | Yes | Yes | Yes | Yes | Yes | 

### Query and write operations
<a name="mongo-apis-dababase-query-write"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| Change streams | [Yes](https://docs.aws.amazon.com//documentdb/latest/developerguide/change_streams.html) | [Yes](https://docs.aws.amazon.com//documentdb/latest/developerguide/change_streams.html) | [Yes](https://docs.aws.amazon.com//documentdb/latest/developerguide/change_streams.html) | [Yes](https://docs.aws.amazon.com//documentdb/latest/developerguide/change_streams.html) | No | 
| delete | Yes | Yes | Yes | Yes | Yes | 
| find | Yes | Yes | Yes | Yes | Yes | 
| findAndModify | Yes | Yes | Yes | Yes | Yes | 
| getLastError | No | No | No | No | No | 
| getMore | Yes | Yes | Yes | Yes | Yes | 
| getPrevError | No | No | No | No | No | 
| GridFS | Yes | Yes | Yes | Yes | No | 
| insert | Yes | Yes | Yes | Yes | Yes | 
| parallelCollectionScan | No | No | No | No | No | 
| resetError | No | No | No | No | No | 
| update | Yes | Yes | Yes | Yes | Yes | 
| ReplaceOne | Yes | Yes | Yes | Yes | Yes | 

### Role management commands
<a name="mongo-apis-database-role-management"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| createRole | Yes | Yes | Yes | Yes | No | 
| dropAllRolesFromDatabase | Yes | Yes | Yes | Yes | No | 
| dropRole | Yes | Yes | Yes | Yes | No | 
| grantRolesToRole | Yes | Yes | Yes | Yes | No | 
| revokeRolesFromRole | Yes | Yes | Yes | Yes | No | 
| revokePrivilegesFromRole | Yes | Yes | Yes | Yes | No | 
| rolesInfo | Yes | Yes | Yes | Yes | No | 
| updateRole | Yes | Yes | Yes | Yes | No | 

### Sessions commands
<a name="mongo-apis-dababase-sessions"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| abortTransaction | No | Yes | Yes | Yes | No | 
| commitTransaction | No | Yes | Yes | Yes | No | 
| endSessions | No | No | No | No | No | 
|  killAllSessions | No | Yes | Yes | Yes | No | 
| killAllSessionsByPattern | No | No | No | No | No | 
| killSessions | No | Yes | Yes | Yes | No | 
| refreshSessions | No | No | No | No | No | 
| startSession | No | Yes | Yes | Yes | No | 

### User management
<a name="mongo-apis-dababase-user-management"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| createUser | Yes | Yes | Yes | Yes | Yes | 
| dropAllUsersFromDatabase | Yes | Yes | Yes | Yes | Yes | 
| dropUser | Yes | Yes | Yes | Yes | Yes | 
| grantRolesToUser | Yes | Yes | Yes | Yes | Yes | 
| revokeRolesFromUser | Yes | Yes | Yes | Yes | Yes | 
| updateUser | Yes | Yes | Yes | Yes | Yes | 
| usersInfo | Yes | Yes | Yes | Yes | Yes | 

### Sharding commands
<a name="mongo-apis-dababase-sharding"></a>


| Command | Elastic cluster | 
| --- | --- | 
| abortReshardCollection | No | 
| addShard | No | 
| addShardToZone | No | 
| balancerCollectionStatus | No | 
| balancerStart | No | 
| balancerStatus | No | 
| balancerStop | No | 
| checkShardingIndex | No | 
| clearJumboFlag | No | 
| cleanupOrphaned | No | 
| cleanupReshardCollection | No | 
| commitReshardCollection | No | 
| enableSharding | Yes | 
| flushRouterConfig | No | 
| getShardMap | No | 
| getShardVersion | No | 
| isdbgrid | No | 
| listShards | No | 
| medianKey | No | 
| moveChunk | No | 
| movePrimary | No | 
| mergeChunks | No | 
| refineCollectionShardKey | No | 
| removeShard | No | 
| removeShardFromZone | No | 
| reshardCollection | No | 
| setAllowMigrations | No | 
| setShardVersion | No | 
| shardCollection | Yes | 
| shardingState | No | 
| split | No | 
| splitVector | No | 
| unsetSharding | No | 
| updateZoneKeyRange | No | 

## Query and projection operators
<a name="mongo-apis-query"></a>

**Topics**
+ [Array Operators](#mongo-apis-query-array-operators)
+ [Bitwise operators](#mongo-apis-query-bitwise-operators)
+ [Comment operator](#mongo-apis-query-comment-operator)
+ [Comparison operators](#mongo-apis-query-comparison-operators)
+ [Element operators](#mongo-apis-query-element-operators)
+ [Evaluation query operators](#mongo-apis-query-evaluation-operators)
+ [Logical operators](#mongo-apis-query-logical-operators)
+ [Projection operators](#mongo-apis-projection-operators)

### Array Operators
<a name="mongo-apis-query-array-operators"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$all](all.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$elemMatch](elemMatch.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$size](size-query.md) | Yes | Yes | Yes | Yes | Yes | 

### Bitwise operators
<a name="mongo-apis-query-bitwise-operators"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$bitsAllSet](bitsAllSet.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$bitsAnySet](bitsAnySet.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$bitsAllClear](bitsAllClear.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$bitsAnyClear](bitsAnyClear.md) | Yes | Yes | Yes | Yes | Yes | 

### Comment operator
<a name="mongo-apis-query-comment-operator"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$comment](comment.md) | Yes | Yes | Yes | Yes | Yes | 

### Comparison operators
<a name="mongo-apis-query-comparison-operators"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$eq](eq.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$gt](gt.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$gte](gte.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$in](in.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$lt](lt.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$lte](lte.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$ne](ne.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$nin](nin.md) | Yes | Yes | Yes | Yes | Yes | 

### Element operators
<a name="mongo-apis-query-element-operators"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$exists](exists.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$type](type.md) | Yes | Yes | Yes | Yes | Yes | 

### Evaluation query operators
<a name="mongo-apis-query-evaluation-operators"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$expr](expr.md) | No | Yes | Yes | Yes | No | 
| [\$jsonSchema](jsonSchema.md) | No | Yes | Yes | Yes | No | 
| [\$mod](mod-query.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$regex](regex.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$text](text.md) | No | No | Yes | Yes | No | 
| \$where | No | No | No | No | No | 

### Logical operators
<a name="mongo-apis-query-logical-operators"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$and](and.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$nor](nor.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$not](not.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$or](or.md) | Yes | Yes | Yes | Yes | Yes | 

### Projection operators
<a name="mongo-apis-projection-operators"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$](dollar-projection.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$elemMatch](elemMatch.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$meta](meta.md) | No | No | Yes | Yes | No | 
| [\$slice](slice-projection.md) | Yes | Yes | Yes | Yes | Yes | 

## Update operators
<a name="mongo-apis-update"></a>

**Topics**
+ [Array operators](#mongo-apis-update-array)
+ [Bitwise operators](#mongo-apis-update-bitwise)
+ [Field operators](#mongo-apis-update-field)
+ [Update modifiers](#mongo-apis-update-modifiers)

### Array operators
<a name="mongo-apis-update-array"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$](dollar-update.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$[]](dollarBrackets-update.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$[<identifier>]](dollarIdentifier-update.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$addToSet](addToSet.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$pop](pop.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$pullAll](pullAll.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$pull](pull.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$push](push.md) | Yes | Yes | Yes | Yes | Yes | 

### Bitwise operators
<a name="mongo-apis-update-bitwise"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$bit](bit.md) | Yes | Yes | Yes | Yes | Yes | 

### Field operators
<a name="mongo-apis-update-field"></a>


| Operator | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$currentDate](currentDate.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$inc](inc.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$max](max-update.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$min](min-update.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$mul](mul.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$rename](rename.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$set](set-update.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$setOnInsert](setOnInsert.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$unset](unset-update.md) | Yes | Yes | Yes | Yes | Yes | 

### Update modifiers
<a name="mongo-apis-update-modifiers"></a>


| Operator | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$each](each.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$position](position.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$slice](slice-update.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$sort](sort-update.md) | Yes | Yes | Yes | Yes | Yes | 

## Geospatial
<a name="mongo-apis-geospatial"></a>

### Geometry specifiers
<a name="mongo-apis-geospatial-geometry-specifiers"></a>


| Query Selectors | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| \$box | No | No | No | No | No | 
| \$center | No | No | No | No | No | 
| \$centerSphere | No | No | No | No | No | 
| [\$geometry](geometry.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$maxDistance](maxDistance.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$minDistance](minDistance.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$nearSphere](nearSphere.md) | Yes | Yes | Yes | Yes | Yes | 
| \$polygon | No | No | No | No | No | 
| \$uniqueDocs | No | No | No | No | No | 

### Query selectors
<a name="mongo-apis-geospatial-query-selectors"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$geoIntersects](geoIntersects.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$geoWithin](geoWithin.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$near](near.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$nearSphere](nearSphere.md) | Yes | Yes | Yes | Yes | Yes | 
| \$polygon | No | No | No | No | No | 
| \$uniqueDocs | No | No | No | No | No | 

## Cursor methods
<a name="mongo-apis-cursor"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| cursor.batchSize() | Yes | Yes | Yes | Yes | Yes | 
| cursor.close() | Yes | Yes | Yes | Yes | Yes | 
| cursor.collation() | No | No | No | Yes | No | 
| cursor.comment() | Yes | Yes | Yes | Yes | Yes | 
| cursor.count() | Yes | Yes | Yes | Yes | Yes | 
| cursor.explain() | Yes | Yes | Yes | Yes | No | 
| cursor.forEach() | Yes | Yes | Yes | Yes | Yes | 
| cursor.hasNext() | Yes | Yes | Yes | Yes | Yes | 
| cursor.hint() | Yes | Yes | Yes | Yes | Yes\* | 
| cursor.isClosed() | Yes | Yes | Yes | Yes | Yes | 
| cursor.isExhausted() | Yes | Yes | Yes | Yes | No | 
| cursor.itcount() | Yes | Yes | Yes | Yes | No | 
| cursor.limit() | Yes | Yes | Yes | Yes | No | 
| cursor.map() | Yes | Yes | Yes | Yes | No | 
| cursor.max() | No | No | No | No | No | 
| cursor.maxScan() | Yes | Yes | Yes | Yes | No | 
| cursor.maxTimeMS() | Yes | Yes | Yes | Yes | No | 
| cursor.min() | No | No | No | No | No | 
| cursor.next() | Yes | Yes | Yes | Yes | Yes | 
| cursor.noCursorTimeout() | No | No | No | No | No | 
| cursor.objsLeftInBatch() | Yes | Yes | Yes | Yes | No | 
| cursor.pretty() | Yes | Yes | Yes | Yes | No | 
| cursor.readConcern() | Yes | Yes | Yes | Yes | No | 
| cursor.readPref() | Yes | Yes | Yes | Yes | No | 
| cursor.returnKey() | No | No | No | No | No | 
| cursor.showRecordId() | No | No | No | No | No | 
| cursor.size() | Yes | Yes | Yes | Yes | No | 
| cursor.skip() | Yes | Yes | Yes | Yes | No | 
| cursor.sort() | Yes | Yes | Yes | Yes | No | 
| cursor.tailable() | No | No | No | No | No | 
| cursor.toArray() | Yes | Yes | Yes | Yes | No | 

\* Index `hint` is supported with index expressions. For example, `db.foo.find().hint({x:1})`.

## Aggregation pipeline operators
<a name="mongo-apis-aggregation-pipeline"></a>

**Topics**
+ [Accumulator expressions](#mongo-apis-aggregation-pipeline-accumulator-expressions)
+ [Arithmetic operators](#mongo-apis-aggregation-pipeline-arithmetic)
+ [Array operators](#mongo-apis-aggregation-pipeline-array)
+ [Boolean operators](#mongo-apis-aggregation-pipeline-boolean)
+ [Comparison operators](#mongo-apis-aggregation-pipeline-comparison)
+ [Conditional expression operators](#mongo-apis-aggregation-pipeline-conditional)
+ [Data type operator](#mongo-apis-aggregation-pipeline-data-type)
+ [Data size operator](#mongo-apis-aggregation-pipeline-data-size)
+ [Date operators](#mongo-apis-aggregation-pipeline-date)
+ [Literal operator](#mongo-apis-aggregation-pipeline-literal)
+ [Merge operator](#mongo-apis-aggregation-pipeline-merge)
+ [Natural operator](#mongo-apis-aggregation-pipeline-natural)
+ [Set operators](#mongo-apis-aggregation-pipeline-set)
+ [Stage operators](#mongo-apis-aggregation-pipeline-stage)
+ [String operators](#mongo-apis-aggregation-pipeline-string)
+ [System variables](#mongo-apis-aggregation-pipeline-system-variables)
+ [Text search operator](#mongo-apis-aggregation-pipeline-text-search)
+ [Type conversion operators](#mongo-apis-aggregation-pipeline-type)
+ [Variable operators](#mongo-apis-aggregation-pipeline-variable)
+ [Miscellaneous operators](#mongo-apis-aggregation-pipeline-misc)

### Accumulator expressions
<a name="mongo-apis-aggregation-pipeline-accumulator-expressions"></a>


| Expression | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| \$accumulator | - | - | No | No | No | 
| [\$addToSet](addToSet-aggregation.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$avg](avg.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$count](count.md) | - | - | No | No | No | 
| \$covariancePop | No | No | No | No | No | 
| \$covarianceSamp | No | No | No | No | No | 
| \$denseRank | No | No | No | No | No | 
| \$derivative | No | No | No | No | No | 
| \$documentNumber | No | No | No | No | No | 
| \$expMovingAvg | No | No | No | No | No | 
| [\$first](first.md) | Yes | Yes | Yes | Yes | Yes | 
| \$integral | No | No | No | No | No | 
| [\$last](last.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$max](max.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$min](min.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$push](push-aggregation.md) | Yes | Yes | Yes | Yes | Yes | 
| \$rank | No | No | No | No | No | 
| \$shift | No | No | No | No | No | 
| \$stdDevPop | No | No | No | No | No | 
| \$stdDevSamp | No | No | No | No | No | 
| [\$sum](sum.md) | Yes | Yes | Yes | Yes | Yes | 

### Arithmetic operators
<a name="mongo-apis-aggregation-pipeline-arithmetic"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$abs](abs.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$add](add.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$ceil](ceil.md) | No | Yes | Yes | Yes | Yes | 
| [\$divide](divide.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$exp](exp.md) | No | Yes | Yes | Yes | Yes | 
| [\$floor](floor.md) | No | Yes | Yes | Yes | Yes | 
| [\$ln](ln.md) | No | Yes | Yes | Yes | Yes | 
| [\$log](log.md) | No | Yes | Yes | Yes | Yes | 
| [\$log10](log10.md) | No | Yes | Yes | Yes | Yes | 
| [\$mod](mod.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$multiply](multiply.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$pow](pow.md) | No | No | No | Yes | No | 
| \$round | - | - | No | No | No | 
| [\$sqrt](sqrt.md) | No | Yes | Yes | Yes | Yes | 
| [\$subtract](subtract.md) | Yes | Yes | Yes | Yes | Yes | 
| \$trunc | No | No | No | No | No | 

### Array operators
<a name="mongo-apis-aggregation-pipeline-array"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$arrayElemAt](arrayElemAt.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$arrayToObject](arrayToObject.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$concatArrays](concatArrays.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$filter](filter.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$first](first.md) | - | - | Yes | Yes | No | 
| [\$in](in-aggregation.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$indexOfArray](indexOfArray.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$isArray](isArray.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$last](last.md) | - | - | Yes | Yes | No | 
| [\$objectToArray](objectToArray.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$range](range.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$reverseArray](reverseArray.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$reduce](reduce.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$size](size.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$slice](slice.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$zip](zip.md) | Yes | Yes | Yes | Yes | Yes | 

### Boolean operators
<a name="mongo-apis-aggregation-pipeline-boolean"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$and](and-aggregation.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$not](not-aggregation.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$or](or-aggregation.md) | Yes | Yes | Yes | Yes | Yes | 

### Comparison operators
<a name="mongo-apis-aggregation-pipeline-comparison"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$cmp](cmp.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$eq](eq-aggregation.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$gt](gt-aggregation.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$gte](gte-aggregation.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$lt](lt-aggregation.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$lte](lte-aggregation.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$ne](ne-aggregation.md) | Yes | Yes | Yes | Yes | Yes | 

### Conditional expression operators
<a name="mongo-apis-aggregation-pipeline-conditional"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$cond](cond.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$ifNull](ifNull.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$switch](switch.md) | No | Yes | Yes | Yes | No | 

### Data type operator
<a name="mongo-apis-aggregation-pipeline-data-type"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$type](type-aggregation.md) | Yes | Yes | Yes | Yes | Yes | 

### Data size operator
<a name="mongo-apis-aggregation-pipeline-data-size"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| \$binarySize | - | - | No | No | No | 
| \$bsonSize | - | - | No | No | No | 

### Date operators
<a name="mongo-apis-aggregation-pipeline-date"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$dateAdd](dateAdd.md) | No | No | Yes | Yes | Yes | 
| [\$dateDiff](dateDiff.md) | - | - | Yes | Yes | No | 
| \$dateFromParts | No | No | No | No | No | 
| [\$dateFromString](dateFromString.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$dateSubtract](dateSubtract.md) | No | No | Yes | Yes | Yes | 
| \$dateToParts | No | No | No | No | No | 
| [\$dateToString](dateToString.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$dateTrunc](dateTrunc.md) | - | - | No | Yes | No | 
| [\$dayOfMonth](dayOfMonth.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$dayOfWeek](dayOfWeek.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$dayOfYear](dayOfYear.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$hour](hour.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$isoDayOfWeek](isoDayOfWeek.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$isoWeek](isoWeek.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$isoWeekYear](isoWeekYear.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$millisecond](millisecond.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$minute](minute.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$month](month.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$second](second.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$week](week.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$year](year.md) | Yes | Yes | Yes | Yes | Yes | 

### Literal operator
<a name="mongo-apis-aggregation-pipeline-literal"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$literal](literal.md) | Yes | Yes | Yes | Yes | Yes | 

### Merge operator
<a name="mongo-apis-aggregation-pipeline-merge"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$mergeObjects](mergeObjects.md) | Yes | Yes | Yes | Yes | Yes | 

### Natural operator
<a name="mongo-apis-aggregation-pipeline-natural"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$natural](natural.md) | Yes | Yes | Yes | Yes | Yes | 

### Set operators
<a name="mongo-apis-aggregation-pipeline-set"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$allElementsTrue](allElementsTrue.md) | No | Yes | Yes | Yes | Yes | 
| [\$anyElementTrue](anyElementTrue.md) | No | Yes | Yes | Yes | Yes | 
| [\$setDifference](setDifference.md) | No | Yes | Yes | Yes | Yes | 
| [\$setEquals](setEquals.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$setIntersection](setIntersection.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$setIsSubset](setIsSubset.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$setUnion](setUnion.md) | Yes | Yes | Yes | Yes | Yes | 
| \$setWindowFields | No | No | No | No | No | 

### Stage operators
<a name="mongo-apis-aggregation-pipeline-stage"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$addFields](addFields.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$bucket](bucket.md) | No | No | No | Yes | No | 
| \$bucketAuto | No | No | No | No | 
| [\$changeStream](changeStream.md) | Yes | Yes | Yes | Yes | No | 
| [\$collStats](collStats.md) | No | Yes | Yes | Yes | No | 
| [\$count](count.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$currentOp](currentOp.md) | Yes | Yes | Yes | Yes | Yes | 
| \$facet | No | No | No | No | No | 
| [\$geoNear](geoNear.md) | Yes | Yes | Yes | Yes | Yes | 
| \$graphLookup | No | No | No | No | No | 
| [\$group](group.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$indexStats](indexStats.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$limit](limit.md) | Yes | Yes | Yes | Yes | Yes | 
| \$listLocalSessions | No | No | No | No | No | 
| \$listSessions | No | No | No | No | No | 
| [\$lookup](lookup.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$match](match.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$merge](merge.md) | - | - | No | Yes | No | 
| [\$out](out.md) | Yes | Yes | Yes | Yes | No | 
| \$planCacheStats | - | - | No | No | No | 
| [\$project](project.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$redact](redact.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$replaceRoot](replaceRoot.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$sample](sample.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$set](set-stage.md) | - | - | No | Yes | No | 
| \$setWindowFields | - | - | No | No | No | 
| [\$skip](skip.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$sort](sort.md) | Yes | Yes | Yes | Yes | Yes | 
| \$sortByCount | No | No | No | No | No | 
| \$unionWith | - | - | No | No | No | 
| [\$unset](unset-stage.md) | - | - | No | Yes | No | 
| [\$unwind](unwind.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$replaceWith](replaceWith.md) | No | No | No | Yes | No | 
| [\$vectorSearch](vectorSearch.md) | No | No | No | Yes | No | 

### String operators
<a name="mongo-apis-aggregation-pipeline-string"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$concat](concat.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$indexOfBytes](indexOfBytes.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$indexOfCP](indexOfCP.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$ltrim](ltrim.md) | No | Yes | Yes | Yes | No | 
| [\$regexFind](regexFind.md) | - | - | Yes | Yes | No | 
| [\$regexFindAll](regexFindAll.md) | - | - | Yes | Yes | No | 
| [\$regexMatch](regexMatch.md) | - | - | Yes | Yes | No | 
| [\$replaceAll](replaceAll.md) | - | - | Yes | Yes | No | 
| [\$replaceOne](replaceOne.md) | - | - | Yes | Yes | No | 
| [\$rtrim](rtrim.md) | No | Yes | Yes | Yes | No | 
| [\$split](split.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$strcasecmp](strcasecmp.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$strLenBytes](strLenBytes.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$strLenCP](strLenCP.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$substr](substr.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$substrBytes](substrBytes.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$substrCP](substrCP.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$toLower](toLower.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$toUpper](toUpper.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$trim](trim.md) | No | Yes | Yes | Yes | No | 

### System variables
<a name="mongo-apis-aggregation-pipeline-system-variables"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| \$\$CURRENT | No | No | No | No | No | 
| [\$\$DESCEND](DESCEND.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$\$KEEP](KEEP.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$\$PRUNE](PRUNE.md) | Yes | Yes | Yes | Yes | Yes | 
| \$\$REMOVE | No | No | No | No | No | 
| [\$ROOT](ROOT.md) | Yes | Yes | Yes | Yes | Yes | 

### Text search operator
<a name="mongo-apis-aggregation-pipeline-text-search"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$meta](meta-aggregation.md) | No | No | Yes | Yes | No | 
| [\$search](search.md) | No | No | Yes | Yes | No | 

### Type conversion operators
<a name="mongo-apis-aggregation-pipeline-type"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$convert](convert.md) | No | Yes | Yes | Yes | Yes | 
| \$isNumber | - | - | No | No | No | 
| [\$toBool](toBool.md) | No | Yes | Yes | Yes | Yes | 
| [\$toDate](toDate.md) | No | Yes | Yes | Yes | Yes | 
| [\$toDecimal](toDecimal.md) | No | Yes | Yes | Yes | Yes | 
| [\$toDouble](toDouble.md) | No | Yes | Yes | Yes | Yes | 
| [\$toInt](toInt.md) | No | Yes | Yes | Yes | Yes | 
| [\$toLong](toLong.md) | No | Yes | Yes | Yes | Yes | 
| [\$toObjectId](toObjectId.md) | No | Yes | Yes | Yes | Yes | 
| [\$toString](toString.md) | No | Yes | Yes | Yes | Yes | 

### Variable operators
<a name="mongo-apis-aggregation-pipeline-variable"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| [\$let](let.md) | Yes | Yes | Yes | Yes | Yes | 
| [\$map](map.md) | Yes | Yes | Yes | Yes | Yes | 

### Miscellaneous operators
<a name="mongo-apis-aggregation-pipeline-misc"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| \$getField | - | - | No | No | No | 
| [\$rand](rand.md) | - | - | No | Yes | No | 
| \$sampleRate | - | - | No | No | No | 

## Data types
<a name="mongo-apis-data-types"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| 32-bit Integer (int) | Yes | Yes | Yes | Yes | Yes | 
| 64-bit Integer (long) | Yes | Yes | Yes | Yes | Yes | 
| Array | Yes | Yes | Yes | Yes | Yes | 
| Binary Data | Yes | Yes | Yes | Yes | Yes | 
| Boolean | Yes | Yes | Yes | Yes | Yes | 
| Date | Yes | Yes | Yes | Yes | Yes | 
| DBPointer | No | No | No | No | No | 
| DBRefs | No | No | No | No | No | 
| Decimal128 | Yes | Yes | Yes | Yes | Yes | 
| Double | Yes | Yes | Yes | Yes | Yes | 
| JavaScript | No | No | No | No | No | 
| JavaScript (with scope) | No | No | No | No | No | 
| MaxKey | Yes | Yes | Yes | Yes | Yes | 
| MinKey | Yes | Yes | Yes | Yes | Yes | 
| Null | Yes | Yes | Yes | Yes | Yes | 
| Object | Yes | Yes | Yes | Yes | Yes | 
| ObjectId | Yes | Yes | Yes | Yes | Yes | 
| Regular Expression | Yes | Yes | Yes | Yes | Yes | 
| String | Yes | Yes | Yes | Yes | Yes | 
| Symbol | No | No | No | No | No | 
| Timestamp | Yes | Yes | Yes | Yes | Yes | 
| Undefined | No | No | No | No | No | 

## Indexes and index properties
<a name="mongo-apis-index"></a>

**Topics**
+ [Indexes](#mongo-apis-indexes)
+ [Index properties](#mongo-apis-index-properties)

### Indexes
<a name="mongo-apis-indexes"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| 2dsphere | Yes | Yes | Yes | Yes | Yes | 
| 2d Index | No | No | No | No | No | 
| Compound Index | Yes | Yes | Yes | Yes | Yes | 
| Hashed Index | No | No | No | No | No | 
| Multikey Index | Yes | Yes | Yes | Yes | Yes | 
| Single Field Index | Yes | Yes | Yes | Yes | Yes | 
| Text Index | No | No | Yes | Yes | No | 
| Wildcard | No | No | No | No | No | 

### Index properties
<a name="mongo-apis-index-properties"></a>


| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic cluster | 
| --- | --- | --- | --- | --- | --- | 
| Background | Yes | Yes | Yes | Yes | Yes | 
| Case Insensitive | No | No | No | Yes | No | 
| Hidden | No | No | No | No | No | 
| Partial | No | No | Yes | Yes | No | 
| Sparse | Yes | Yes | Yes | Yes | Yes | 
| Text | No | No | Yes | Yes | No | 
| TTL | Yes | Yes | Yes | Yes | Yes | 
| Unique | Yes | Yes | Yes | Yes | Yes | 
| Vector | No | No | Yes | Yes | No | 

For detailed information about specific MongoDB operators, see the following topics:
+ [Aggregation pipeline operators](mongo-apis-aggregation-pipeline-operators.md)
+ [Geospatial](mongo-apis-geospatial-operators.md)
+ [Projection operators](#mongo-apis-projection-operators)
+ [Update operators](mongo-apis-update-operators.md)
