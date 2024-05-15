# change note

## 7.8.28

- XQLFileManager support parse file description which in annotation format is:

```sql
/*
 ###
 Some description for the file at here.
 ###
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
  methods: `DataRow(Map<String, Object> map)`, `ofJson(String json)`, `ofMap(Map<String, Object> map)`, `toMap()`, `toJson(Function<Object, Object> valueFormatter)`, `toJson()`, `to(Function<DataRow, T> converter)`;
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