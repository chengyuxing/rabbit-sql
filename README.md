# rabbit-sql 使用说明

- 对JDBC的一个薄封装工具类，提供基本的增删改查操作；

- 此库以追求简单稳定高效为目标，不支持查询结果实体映射，返回对象类型统一为[`DataRow`](https://github.com/chengyuxing/rabbit-common/blob/master/src/main/java/rabbit/common/types/DataRow.java)，[`DataRow`](https://github.com/chengyuxing/rabbit-common/blob/master/src/main/java/rabbit/common/types/DataRow.java)提供了了简单的实体互相转换，若需要复杂映射，可自行通过[`DataRow`](https://github.com/chengyuxing/rabbit-common/blob/master/src/main/java/rabbit/common/types/DataRow.java)来实现。

- maven dependency (jdk1.8)

```xml
<dependency>
    <groupId>com.github.chengyuxing</groupId>
    <artifactId>rabbit-sql</artifactId>
    <version>5.1.3</version>
</dependency>
```

## 参数占位符说明

- `:name` (jdbc标准的传名参数写法，参数将被预编译安全处理，参数名为：`name`)

- `${part}` (通用的字符串模版占位符，不进行预编译，用于动态sql的拼接)

  参数名两种格式：

  - `${part}` 和sql中参数占位符一模一样，则不进行任何处理直接进行sql片段的替换；
  - `${..:part}` 名字前多了前缀符号(`..:`)，则对参数类型进行判断，如果类型是**装箱类型数组(String[], Integer[]...)**或**集合(Set, List...)**，则进行展开，并做一定的字符串安全处理。
  - `${...part}` 名字前多了前缀符号(`...`)，则对参数类型进行判断，如果类型是**装箱类型数组(String[], Integer[]...)**或**集合(Set, List...)**，则进行展开。

- 字符串模版中还可以使用传名参数

- 完全参数占位符效果示例：

  sql：

  ```sql
  select ${fields}, ${moreFields} from <tableName> where word in (${words}) or id = :id;
  ```

  参数：

  ```java
  Args<Object> args = Args.<Object>of("id","uuid")
    .add("${fields}", "id, name, address")
    .add("${...moreFields}", Arrays.asList("email", "enable"))
    .add("${..:words}", Arrays.asList("I'm OK!", "book", "warning"));
  ```
  
  最终执行的SQL：
  
  ```sql
  select id, name, address, email, enable from <tableName> where id in ('I''m Ok!', 'book', 'warning') or id = 'uuid';
  ```
  
  

## 外部SQL文件详解(SQLFileManager)

- sql文件中可以包含任意符合标准的注释
- sql文件结尾以`.sql`结尾
- sql文件格式参考```data.sql.template```
- BakiDao中如果配置了属性```sqlFileManager```,则接口中需要写sql的所有方法都可以使用``&文件名.sql名``取地址符来获取sql文件中的sql
- BakiDao中如果配置属性```strictDynamicSqlArg```为`false`（默认为`true`）,则动态sql的参数可以为null、空或键值不存在，否则将抛出异常
- sql文件名格式为``/*[name]*/``，sql文件中可以嵌套sql片段，使用`${片段名}`指定
- sql片段名格式化``/*{name}*/``，sql片段中可以嵌套sql片段，使用`${片段名}`指定
- sql文件将优先寻找sql文件内的sql片段，没找到的情况下，如果配置了属性```constants```，则再从```constants```常量集合中查找

## 动态SQL

- 支持`--#if`和`--#fi`块标签，必须成对出现，类似于Mybatis的if标签；

- 支持`--#choose`和`--#end`块标签，内部可以有多对`--#if`块判断，但只返回第一个条件满足的`--#if`块，效果类似于mybatis的`choose...when`标签；

- 支持的运算符：

  | 运算符 | 说明           |
  | ------ | -------------- |
  | <      | 大于           |
  | >      | 小于           |
  | > =     | 大于等于       |
  | <=     | 小于等于       |
  | ==     | 等于，同 =     |
  | !=     | 不等于，同 <>  |
  | ~      | 正则表查找包含 |
  | !~     | 正则查找不包含 |
  | @      | 正则匹配       |
  | !@     | 正则不匹配     |

- 内置常量：`null` , `blank`(null或空白字符) , `true` , `false`

### 例子

```sql
select *
from test.student t
WHERE
--#choose
  --#if :age < 21
    t.age = 21
  --#fi
  --#if :age <> blank && :age < 90
  and age < 90
  --#fi
--#end
--#if :name != null
  and t.name ~ :name
--#fi
;
```

## 对 *IntelliJ IDEA* 的友好支持

- 配置了数据源的情况下，可以直接选中需要执行的sql右键，点击`Execute`执行sql，参数占位符(`:name`)和sql片段占位符(`${part}`)都会弹出输入框方便填写，直接进行测试sql
  ![](https://github.com/chengyuxing/rabbit-sql/blob/master/img/p.jpg)
  ![](https://github.com/chengyuxing/rabbit-sql/blob/master/img/p2.png)

## Example

### 初始化

```java
dataSource=new HikariDataSource();
dataSource.setJdbcUrl("jdbc:postgresql://127.0.0.1:5432/postgres");
dataSource.setUsername("chengyuxing");
dataSource.setDriverClassName("org.postgresql.Driver");
SQLFileManager manager=new SQLFileManager("pgsql/data.sql, pgsql/other.sql");
BakiDao baki=new BakiDao(dataSource);
baki.setSqlFileManager(manager);
```

### 流查询

```java
try(Stream<DataRow> fruits=baki.query("select * from fruit")){
        fruits.limit(10).forEach(System.out::println);
        }
```

### 分页查询

```java
PagedResource<DataRow> res=baki.<DataRow>query("&pgsql.data.select_user", 1, 10)
        .args(Args.create().set("id", 35))
        .collect(d -> d);
```

### 存储过程

```java
Tx.using(()->baki.call("{call test.fun_query(:c::refcursor)}",
        Args.of("c",Param.IN_OUT("result",OUTParamType.REF_CURSOR)))
        .<List<DataRow>>get(0)
        .stream()
        .map(DataRow::toMap)
        .forEach(System.out::println));
```

