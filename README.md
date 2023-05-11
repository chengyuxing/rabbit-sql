# rabbit-sql

Language: English | [简体中文](README.chs.md)

You don't like sql in xml, don't like xml binding to interfaces?

You don't like tools auto generate too many entities and interfaces file?

You don't like writing [dynamic sql](#Dynamic-sql) in java code?

## Introducing

It's just a small lib, wrapper of **jdbc**, support some basic operation. simple, stable and efficient as the goal(query operation accept sql statement mainly), some features following:

- Basic operation for insert, delete, update, query;
- simple [pageable query](#Paging);
- [stream query](#Stream-query)(java8 **Stream**);
- [execute procedure/function](#Procedure);
- simple [transaction](#Transaction);
- [prepare sql](#Prepare-SQL);
- [sql in file](#XQLFileManager);
- [sql fragment reuse](#XQLFileManager);
- [dynamic sql](#Dynamic-SQL);
- support **spring-boot** framework.

## Maven dependency (jdk1.8)

Maven central

```xml
<dependency>
    <groupId>com.github.chengyuxing</groupId>
    <artifactId>rabbit-sql</artifactId>
    <version>7.1.10</version>
</dependency>
```

## Spring-Boot(2.7+) support

- support rabbit-sql autoconfigure；
- support `application.yml` auto complete；
- compatible with spring jdbc transaction；
- compatible mybatis、spring-data-jpaand so on to use transaction together；

Get some usage from [document](https://github.com/chengyuxing/rabbit-sql-spring-boot-starter).

## IDEA plugin support

plugin market: [Rabbit sql](https://plugins.jetbrains.com/plugin/21403-rabbit-sql).

## Quick start

### Init

```java
dataSource=new HikariDataSource();
...
BakiDao baki=new BakiDao(dataSource);
```

### Query

Use [baki](#BakiDao)'s  `query` operation，`query` returns a **query executor**，support some return type like：`Stream`，`Optional` and so on.

```java
baki.query("select … where id = :id").arg("id", "1")
```

```java
baki.query("&my.users")
```

```mermaid
flowchart LR;
A["#quot;select ...#quot;"] --> Baki["query(#quot;#quot;)"];
B[&my.users] --> X[XQLFileManager];
X --> Baki;
click X href "#XQLFileManager" "go to defenition"
```

> Except accept sql statement, also support accept sql by name, name start with `&` to get sql from [sql file manager](#XQLFileManager).

#### Example

##### Stream-query

```java
try(Stream<DataRow> fruits=baki.query("select * from fruit").stream()){
        fruits.limit(10).forEach(System.out::println);
        }
```

> Query will not truly execute until invoke **Stream terminal operation**(e.g `foreach()` ), use jdk7 **try-with-resource** to release connection when query complete.

##### Paging

Default pageable query will auto generate **paging statement** and **count** statement by database.

Built-in support oracle, mysql, postgresql, sqlite, mariadb, db2, or extends class `com.github.chengyuxing.sql.page.PageHelperProvider` and set to [BakiDao](#BakiDao) get support.

```java
PagedResource<DataRow> resource = baki.query("select ... where id < :id")
                .arg("id", 8)
                .pageable(1, 7)
                .collect();
```

##### Custom paging

`/pgsql/data.sql`

```sql
/*[custom_paged]*/
select * from test.region
where id > :id limit :limit offset :offset;
```

```java
PagedResource<DataRow> res = baki.query("&data.custom_paged")
  		          .pageable(1, 7)
                .count("select count(*) ... where id > :id")
                .disableDefaultPageSql()
                .collect();
```

> `disableDefaultPageSql()` will not wrap sql to generate paging statement of name custom_paged.
>
> **count** statement is required now.

##### Procedure

```java
baki.call("{call test.fun_query(:c::refcursor)}",
        Args.of("c",Param.IN_OUT("result",OUTParamType.REF_CURSOR)))
        .<List<DataRow>>getFirstAs()
        .stream()
        .forEach(System.out::println);
```

> If **postgresql**, you must use transaction when returns cursor.

### Update & Insert

I'm going to focus here on the update operation, use [baki](#BakiDao)'s  `update` operation, `update` returns a **update executor**，some details following:

- **safe** property: get all table fields before execute update, and remove updated data fields which not exist in table fields;

  > Notice, recommend do not use this property for improve performance if you 100% fully know the data you need will be updated.
  >
  > Same as **insert** operation.

- **fast** property: in fact, is invoke jdbc batch execute, it's not prepared sql, so not support blob file.

  > It's not  recommend unless you need to batch execute more than 1000 rows of data.
  >
  > Same as **insert** operation.

The 2nd arg `where` of `update` operation, condition is static if statement not contains named parameter, all data will be updated on static condition; if statement contains named parameter like: `id = :id` , all data will be updated dynamically by every id parameter value.

##### Example

Data:`[{name: 'cyx', 'age': 29, id: 13}, ...]`;

Condition: `id = :id`;

`update` operation can find arg which in condition and generate correct update statement:

```sql
update ... set name = :name, age = :age where id = :id;
```

### Transaction

Use of transactions follows thread isolation:

```java
Tx.using(()->{
  baki.update(...);
  baki.delete(...);
  baki.insert(...);
  ......
});
```

## SQL parameter holder

### Prepare-SQL

Prepare sql support named parameter style, e.g: 

`:name` (jdbc standard named parameter syntax, sql will be prepare saftly, parameter name is `name` )

> Named parameter will be compile to `?`, Recommend to use prepare sql for avoid sql injection.

### String template

`${[:]name}` (string template holder, not prepare, use for sql fragment reuse)

2 styles：

- `${part}`: if value type is **boxed type array(String[], Integer[]...)** or **collection (Set, List...)**, just expand value and replace.
- `${:part}`: name start with `:`, if value type is **boxed type array(String[], Integer[]...)** or **collection(Set, List...)**, expand value and safe quote, then replace.

#### Example

sql:

```sql
select ${fields}, ${moreFields} from ... where word in (${:words}) or id = :id;
```

args:

```java
Args.<Object>of("id","uuid")
  .add("fields", "id, name, address")
  .add("moreFields", Arrays.asList("email", "enable"))
  .add("words", Arrays.asList("I'm OK!", "book", "warning"));
```

generate sql:

```sql
select id, name, address, email, enable from ... where id in ('I''m Ok!', 'book', 'warning') or id = ?;
```

## Dynamic-SQL

Dynamic SQL depends on [XQLFileManager](#XQLFileManager), based on resolve special annotation mark, dynamic compile without breaking sql file standards, a simple dynamic sql example following:

```sql
/*[q2]*/
select * from test.user t
where
--#if :names <> blank
	-- #for name,idx of :names delimiter ' and ' filter ${idx} > 0 && ${name} ~ 'o'
		t.name = ${:name.id}
	-- #end
--#fi
--#if :id > -1
        and id = :id
--#fi
...
;
```

### Annotation mark

Annotation mark must be pair and follows **open-close** tag:

#### if

Similar to Mybatis's  `if`  tag, support nest `if`，`choose`，`switch`，`for` :

```sql
--#if expression
       --#if expression
       ...
       --#fi
--#fi
```

#### switch

Similar to program language `switch`'s logic, support nest `if` tag:

```sql
--#switch :name
       --#case value
       		--#if expression
       			...
       		--#fi
       --#break
       ...
       --#default
       	...
       --#break
--#end
```

#### choose

Similar to Mybatis's `choose...when` tag, support nest `if` tag:

```sql
--#choose
       --#when expression
       		--#if expression
       			...
       		--#fi
       --#break
       ...
       --#default
       	...
       --#break
--#end
```

#### for

Can not nest any tag, but can nested in `if` tag, similar to program language `foreach`'s logic, and more features:

```sql
--#for expression
	...
--#end
```

**For expression** syntax:

Keywords: `of` `delimiter` `filter`

```sql
item[,idx] of :list [|pipe1| ... ] [delimiter ','] [filter ${item.name}[|pipe1|... ] <> blank]
```

a complete for expression has 3 part:

Iterator body:

```sql
item[,idx] of :list [|pipe1| ... ]
```

> `item` is current item，`idx` is current index(0 is first), support custom naming but different.

Iterator body delimiter (optional):

```sql
delimiter ','
```
> Default `, `.

Filter (optional): 

```sql
filter ${item.name}[|pipe1|... ] <> blank
```

> Iterate to generate sql by matched item if `filter` configured;
> `filter` expression is a little different from standard, because **compared value** is from for expression, so do not use named parameter , use `${}` instead.

`[...]` means optional item, a simple expression e.g:

  ```sql
  for item of :list
  ```

Get details follows [example](#Dynamic-SQL).

### Expression-script

Left start with `:` is data's key.

Right is compared value.

 A simple expression syntax following: 

```sql
!(:id >= 0 || :name | length <= 3) && :age > 21
```

#### Supported operator

| Operator | Means               |
| -------- | ------------------- |
| <        | less than           |
| >        | great than          |
| >=       | great than or equal |
| <=       | less than or equal  |
| ==, =    | equal               |
| !=, <>   | not equal           |
| ~        | regex find          |
| !~       | regex not find      |
| @        | regex match         |
| !@       | regex not match     |

- Support logic symbol: `||`, `&&`, `!` ;

- Support nest bracket: `(`, `)` ;

- Support data type: string(`""`、`''`), number(12、3.14), boolean(`true` , `false`);

- Built-in constants: `null` , `blank` (`null`, empty string、empty array、empty collection);

> use custom **pipe** to implement more features.

#### Pipe

Syntax look like `:id | upper | is_id_card | ...` e.g: 

```mermaid
flowchart LR;
A[abc] --upper--> B[ABC];
B --is_id_card--> C[false];
C --pipeN--> D[...]
```

```sql
-- get value by name through length pipe and compare with number 3
:name|length <= 3
```

Implement  `com.github.chengyuxing.common.script.IPipe`  interface and add to [XQLFileManager](#XQLFileManager)  to use pipe.

## Appendix

A little important details you need to know.

### BakiDao

Default implement of interface **Baki**, support some basic operation.

- If [XQLFileManager](#XQLFileManager) configured ,  you can manage sql in file and support [dynamic sql](#Dynamic-SQL);

- Default named parameter start with `:` , it can be customized by specific property `namedParamPrefix`, e.g:

  ```sql
  where id = ?id
  ```

  > :warning: Named parameter syntax has nothing to do with dynamic sql expression，[expression](#Expression-script)'s value of key also start with `:`.

- if [pageable query](#paging) not support your database, implement custom page helper provider to property `globalPageHelperProvider` get support.

### XQLFileManager

SQL file manager extends standard sql annotation implement more features, for support [dynamic sql](#Dynamic-SQL) and expression scripts logic judgment without breaking standard sql structure, also it's more powerful SQL file resolver.

you can get sql syntax highlight, intelligent suggestions and error check when using sql develop tools cause support sql file with extension `.sql`, dba developer     work with java developer together so easy.

Supported file extension with `.sql` or `.xql`, you can write any standard sql annotation in file, format reference `data.xql.template`.

Every managed sql file must follows **"k-v"** structure, e.g:

`my.sql`

```sql
/*[query]*/
select * from test."user" t ${part1};

/*part 1*/
/*{part1}*/
where id = :id
${order};

/*{order}*/
order by id;

...
```

- Sql object name formatter is `/*[name]*/`, sql object supports nest sql fragment by using `${fragment name}` holder; 

- Sql fragment name formatter is `/*{name}*/` , sql fragment supports nest sql fragment by using `${fragment name}` holder to reuse, as above example `my.sql`:

  ```sql
  select * from test."user" t where id = :id order by id;
  ```

#### Constructor

- **new XQLFileManager()**

  If source root `.../src/main/resources` contains file what is named `xql-file-manager.properties` , optional properties will be init by this file, the default options in `sql-file-manager.properties` :

  ```properties
  # Format: multi xql file split by ',' symbol and file name is alias default, e,g:
  # filenames=data.xql,system.xql
  filenames=
  
  # Format: multi xql file configure the custom alias, e.g:
  # files.dt=data.sql
  # files.sys=system.sql
  
  # Multi sql fragment delimiter symbol in xql file, ';' is the default also standard.
  # Notice: if your sql fragment is ddl or procedure, maybe one fragment contains
  # more ';' and it's not a delimiter, you have to change delimiter to another like ';;'.
  delimiter=;
  
  # UTF-8 is the default.
  charset=UTF-8
  
  constants=
  pipes=
  namedParamPrefix=:
  highlightSql=false
  checkPeriod=30
  checkModified=false
  ```

#### Options

- **files**

  Sql file mapping dictionary, key is alias, value is sql file name, you can get sql statement  by `alias.your_sql_name` when sql file added, as above example: `my.sql`;

- **filenames**

  sql file name list, default alias is file name, exclude suffix;

- **pipeInstances/pipes**

  Custom [pipe](#Pipe) dictionary, **key** is pipe name, **value** is pipe class, for dynamic sql expression's value, get more [dynamic sql expression](#Expression-script)'s features by implement custom pipe;

- **delimiter**

  Sql file **"k-v"** structure delimiter **default `;`**, follows standard multi sql structure delimiter by `;`, but there is a condition, if you have plsql in file e.g: `create function...` or `create procedure...`, it will be multi sql statement in one sql object, you need specific custom delimiter for resolve correctly:

  - e.g ( `;;`) double semicolon;
  - `null` or `""` : every sql object must follows without delimiter.

- **checkModified**

  Listening sql file modifiable for reload with default period 30 seconds if `true`, recommend set `false` where in production environment.
