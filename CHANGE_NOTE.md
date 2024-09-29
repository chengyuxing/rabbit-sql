# change note

# 7.11.20

- Some optimized.

# 7.11.19

- Mapper support detect method name to execute sql.

## 7.11.18

- New method: `BakiDao#proxyXQLMapper`;
- Add support for interface mapping to xql file.

## 7.11.17

- `SaveExecutor#fast` departed;
- Some optimized.

## 7.11.16

- Some optimized.

## 7.11.15

- Some optimized.

## 7.11.14

- New method: `BakiDao#queryTimeout`.

## 7.11.13

- New method: `BakiDao#sqlWatcher`.

## 7.11.12

- Some optimized.

## 7.11.11

- Some optimized.

## 7.11.10

- Some optimized.

## 7.11.9

- New method: `BakiDao#setPageKey`, `BakiDao#setSizeKey`, `QueryExecutor#pageable`.

## 7.11.8

- Fixed dynamic sql #for-#done bug.

## 7.11.7

- Fixed XQL File Manager parsing block annotation bug.

## 7.11.6

- Some optimized.

## 7.11.5

- Parsing sql file description optimized.

## 7.11.4

- Some optimized.

## 7.11.3

- Fixed dynamic sql parsing number token bug.

## 7.11.2

- Fixed `#for-#done` missing pipes verify bug.
- `#for-#done` loop sql body named parameter parsing optimized.
- `#for-#done` removed named parameter prefix `_for.`.

## 7.11.1

- Support verify invalid nest expression.
- Support verify non-pair expression.
- Support `switch-case` and `choose-when` branch nest `switch` and `choose` expression.
- Fixed dynamic sql parsed result concat bugs.
- Fixed dynamic sql parse bugs.

## 7.10.1

- Dynamic sql Flow-control syntax verify optimized.

## 7.10.0

- Dynamic sql parse optimized.
- XQL File Manager support dynamic sql script syntax verify.

## 7.9.18

- Fixed `#for` pipes line bug.

## 7.9.16

- Bug fixed.

## 7.9.15

- Dynamic sql #case branch support multiple values: e.g. `#case 'a', 3.14, c` .
- More strict syntax check for dynamic sql script.
- Some optimized.

## 7.9.14

- Dynamic sql parser optimized.
- Dynamic sql parse supports `if-else-fi` block.
- Some optimized.

## 7.9.13

- Named parameter sql parsing optimized.

## 7.9.12

- XQLFileManager yml config supports Variable placeholder: `${env.Name}` (`System.getenv`)
- Fixed XQLFileManager yml in windows file system error.

## 7.9.10

- Some optimized.

## 7.9.9

- Fixed named parameter in annotation occurs error.
- Some optimized.

## 7.9.8

- Support entity mapping.
- Formatter function optimized.

## 7.9.7

- 5Fixed SqlHighlighter bug.

## 7.9.6

- Template format support function.
- `SqlHighlighter` optimized.
- `JdbcUtil` support `MostDateTime`.
- New method: `Executor#executeBatch`.
- Query optimized.

## 7.9.5

- Query optimized.

## 7.9.4

- `BakiDao#autoXFMConfig` set default to false.

## 7.9.3

- Some optimized and bug fixed.

## 7.9.2

- Some optimized and bug fixed.

## 7.9.1

- Some optimized.

## 7.9.0

- Some code optimized and bugs fixed.

## 7.8.33

- Optimized.

## 7.8.32

- Fixed `BakiDao#autoXFMConfig` bug when any config not exists.

## 7.8.31

- Fixed doc.

## 7.8.30

- Support auto configure xql file manager by databaseId.

## 7.8.29

- XQLFileManager support parse file description which in annotation format is:

```sql
/*
 @@@
 Some description for the file at here.
 @@@
 */
```

## 7.8.26

- XQLFileManager sql fragment description parsing optimized.

## 7.8.25

- XQLFileManager add support for parsing sql fragment description `/*#some description#*/` which around sql name.
- New method: `XQLFileManager#getSqlObject(String name)`.

## 7.8.24

- Fixed `XQLFileManager#contains` bug.
- Fixed `XQLFileManager#parse` bug.

## 7.8.23

- Fixed sql generate bug: `SqlUtil#replaceSqlSubstr`.

## 7.8.22

- Fixed `DataRow#toEntity` bug.

## 7.8.20

- Some optimized.

## 7.8.17

- Fixed OraclePageHelper#start() start rownum from 0.

## 7.8.16

- XQLFileManager auto generate sql alias allows includes dot(.)

## 7.8.15

- Fixed `SqlUtil#formatObject` value is null returns empty string.

## 7.8.14

- Some optimized.

## 7.8.13

- Fixed XQLFileManager#mergeSqlTemplate bug occurs line annotation into sql statement.
- Remove unused methods of XQLFileManager.

## 7.8.12

- Sql highlight optimized;

## 7.8.10

- New method: `DataRow#toKeyValue()`;
- Rename **DateTimes** to **MostDateTime**;
- `ObjectUtil#map2entity` remove supports for detected json string to collection or map;
- **DataRow** remove
  methods: `DataRow(Map<String, Object> map)`, `ofJson(String json)`, `ofMap(Map<String, Object> map)`, `toMap()`,
  `toJson(Function<Object, Object> valueFormatter)`, `toJson()`, `to(Function<DataRow, T> converter)`;
- `SqlUtil#quoteFormatValue` remove supports for Map, Collection, JavaBean json serialized;
- `JdbcUtil#setStatementValue` remove supports for Map, Collection, JavaBean json serialized;
- `Args` remove methods: `ofJson()`, `ofMap()`;
- Sql highlight optimized;
- PagedArgs optimized;
- Some bug fixed and code optimized.

## 7.8.9

- Dynamic sql repair syntax error logic optimized.

## 7.8.8

- Dynamic sql repair syntax error logic optimized.
- Sql highlight optimized.

## 7.8.6

- new field: `BakiDao#afterParseDynamicSql`;
- Some optimized.

## 7.8.5

- Update rabbit-common to v2.6.2;
- Some optimized.

## 7.8.3

- Update rabbit-common to v2.6.1;
- New method: `DataRow#toJson(valueFormat)`.

## 7.8.2

- Update methods document;
- New method: `SqlUtil#parseValue`;
- `SqlGenerator#genetateSql` support `Variable` type value;
- New method: new method: `PagedResource#empty`;
- Fixed dynamic sql occurs `where...orderby` bug.
- Some code optimized.

## 7.8.0

- released.