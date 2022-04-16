# rabbit-sql 使用说明

- 对JDBC的一个薄封装工具类，提供基本的增删改查操作；
- 此库以追求简单稳定高效为目标，不支持查询结果实体映射，返回对象类型统一为[`DataRow`](https://github.com/chengyuxing/rabbit-common/blob/master/src/main/java/rabbit/common/types/DataRow.java)，[`DataRow`](https://github.com/chengyuxing/rabbit-common/blob/master/src/main/java/rabbit/common/types/DataRow.java)提供了了简单的实体互相转换，若需要复杂映射，可自行通过[`DataRow`](https://github.com/chengyuxing/rabbit-common/blob/master/src/main/java/rabbit/common/types/DataRow.java)来实现。
- maven dependency (jdk1.8)

```xml
<dependency>
    <groupId>com.github.chengyuxing</groupId>
    <artifactId>rabbit-sql</artifactId>
    <version>6.1.7</version>
</dependency>
```

## 参数占位符说明

- `:name` (jdbc标准的传名参数写法，参数将被预编译安全处理，参数名为：`name`)

- `${[:]name}` (通用的字符串模版占位符，不进行预编译，用于动态sql的拼接)

  字符串模版参数名两种格式：

  - `${part}` 如果类型是**装箱类型数组(String[], Integer[]...)**或**集合(Set, List...)**，则先展开（逗号分割），再进行sql片段的替换；
  - `${:part}` 名字前多了前缀符号(`:`)，如果类型是**装箱类型数组(String[], Integer[]...)**或**集合(Set, List...)**，则先展开（逗号分隔），并做一定的字符串安全处理，再进行sql片段的替换。

- 字符串模版中还可以使用传名参数

- 完全参数占位符效果示例：

  sql：

  ```sql
  select ${fields}, ${moreFields} from <tableName> where word in (${:words}) or id = :id;
  ```

  参数：

  ```java
  Args<Object> args = Args.<Object>of("id","uuid")
    .add("fields", "id, name, address")
    .add("moreFields", Arrays.asList("email", "enable"))
    .add("words", Arrays.asList("I'm OK!", "book", "warning"));
  ```
  
  最终执行的SQL：
  
  ```sql
  select id, name, address, email, enable from <tableName> where id in ('I''m Ok!', 'book', 'warning') or id = 'uuid';
  ```

## 接口实现BakiDao

### 构造函数

BakiDao(DataSource dataSource)

### 可选属性

- [**xqlFileManager**](#XQLFileManager)

  接口中需要写sql的所有方法都可以使用``&别名或文件包路径.sql名``取地址符来获取sql文件中的sql。

- **debugFullSql**

  默认值: false

  debug模式下打印拼接完整的sql。

- **strictDynamicSqlArg**

  默认值: true

  如果为false，则动态sql的参数可以为null、空或键值不存在，否则将抛出异常。

- **checkParameterType**

  默认值: true

  如果为true，则检查预编译参数对应数据库映射出来的真实java类型，可实现参数智能匹配合适的类型；

  例如：PostgreSQL中，字段类型为`jsonb`，参数为一个`HashMap<>()`，则将对参数进行json序列化并插入；

  ⚠️ 由于jdbc驱动实现问题，此特性暂不支持Oracle和某些数据库，如果发生异常，请将此属性设置为false。
  

## XQLFileManager

文件结尾以`.sql`或`.xql`结尾，文件中可以包含任意符合标准的注释，格式参考```data.xql.template```；

对象名格式为``/*[name]*/``，sql文件中可以嵌套sql片段，使用`${片段名}`指定;

片段名格式化``/*{name}*/``，sql片段中可以嵌套sql片段，使用`${片段名}`指定。

IOC容器配置例子，这里使用**Nutz**框架的ioc容器，其他框架同理：

```javascript
xqlFileManager: {
        type: 'com.github.chengyuxing.sql.XQLFileManager',
        fields: {
            constants: {
                db: "test"
            },
            files: {
                sys: 'pgsql/test.sql',
                mac: 'file:/Users/chengyuxing/Downloads/local.sql'
            }
        }, events: {
            create: 'init'
        }
    }
```

#### 构造函数

- XQLFileManager()

- XQLFileManager(Map<String, String> files)


#### 属性

- **checkModified**

  如果为`true`，则开启sql文件修改监听器，默认30秒检测一次，如果修改过则重新加载，生产环境建议设置为`false`。

- **checkPeriod**

  sql文件修改监听检查周期，默认为30秒。

- **charset**

  设置解析SQL文件使用的编码，默认**UTF-8**。

- **delimiter**

  解析文件时的SQL块分隔符。

  每个文件的sql片段块解析分隔符，每一段完整的sql根据此设置来进行区分，**默认是单个分号（;）**遵循标准sql文件多段sql分隔符，但是有一种情况，如果sql文件内有psql：**create function...** 或 **create procedure...**等， 内部会包含多段sql多个分号，为防止解析异常，单独设置自定义的分隔符，例如（;;）双分号，也是标准sql所支持的，此处别有他用。

- **constants**

  全局SQL字符串模版常量

  sql文件将优先寻找sql文件内的sql片段，没找到的情况下，如果配置了属性```constants```，则再从```constants```常量集合中查找；

  BakiDao中执行sql方法参数中如果没有找到sql字符串模版，则自动查找并替换`constants`中的常量，例如：

  ```java
  baki.update("${db}.user",...);
  // --> update test.user ...
  ```

- **files** 

  命名别名的文件路径集合字典

  取sql写法，**&文件别名.sql名**：`&sys.getUser`

#### 动态SQL

- 支持`--#if`和`--#fi`块标签，必须成对出现，类似于Mybatis的`if`标签；

- 支持`--#switch`和`--#end`块标签，内部为：`--#case`, `--#default`和`--#break`，效果类似于程序代码的`switch`；

- 支持`--#choose`和`--#end`块标签，内部为`--#when`, `--#default`和`--#break`，效果类似于mybatis的`choose...when`标签；

- 支持的运算符：

  | 运算符 | 说明           |
  | ------ | -------------- |
  | <      | 大于           |
  | >      | 小于           |
  | >=     | 大于等于       |
  | <=     | 小于等于       |
  | ==, =  | 等于           |
  | !=, <> | 不等于         |
  | ~      | 正则表查找包含 |
  | !~     | 正则查找不包含 |
  | @      | 正则匹配       |
  | !@     | 正则不匹配     |

- 支持的逻辑符：`||`, `&&`, `!`

- 支持嵌套括号：`(`, `)`

- 内置常量：`null` , `blank`(null、空白字符、空数组、空集合) , `true` , `false`

**表达式语法例子如下：**

```java
!(:id >= 0 || :name <> blank) && :age <= 21
```

**动态SQL具体语法例子如下：**

```sql
select *
from test.region t
where t.enable = true
--#if :a <> blank
      and t.a = :a
      --#if :a1 <> blank && :a1 = 90
        and t.a1 = :a1
        and t.a1 = :a1
        and t.a1 = :a1
      --#fi
      --#if :a2 <> blank
        and t.a2 = :a2
          --#choose
              --#when :xx <> blank
                and t.xx = :xx
              --#break
              --#when :yy <> blank
                and t.yy = :yy
              --#break
              --#default
                and t.zz = :zz
              --#break
          --#end
      --#fi
--#fi
--#choose
      --#when :x <> blank
        and t.x = :x
      --#break
      --#when :y <> blank
        and t.y = :y
      --#break
--#end
--#switch :name
      --#case blank
        and t.name = 'blank'
      --#break
      --#case 'chengyuxing'
        and t.name = 'chengyuxing'
      --#break
      --#default
        and t.name = 'unset'
      --#break
--#end
--#if :b <> blank
    and t.b = :b
--#fi
--#if :c <> blank
      --#if :c1 <> blank
        and t.c1 = :c1
          --#if :cc1 <> blank
            and t.cc1 = :cc1
          --#fi
          --#if :cc2 <> blank
            and t.cc2 = :cc2
          --#fi
      --#fi
      --#if :c2 <> blank
        and t.c2 = :c2
      --#fi
      and cc = :cc
--#fi
--#choose
      --#when :e <> blank
        and t.e = :e
        and t.ee = :e
        and t.eee = :e
      --#break
      --#when :f <> blank
        and t.f = :f
        --#if :ff <> blank
          and t.ff = :ff
          and t.ff2 = :ff
        --#fi
      --#break
      --#when :g <> blank
        and t.g = :g
      --#break
--#end
and x = :x
;
```

⚠️ **case**和**when**分支中可以嵌套**if**语句，但不可以嵌套**choose**和**switch**，**if**语句中可以嵌套**choose**和**switch**，以此类推，理论上可以无限嵌套，但过于复杂，不太推荐，**3层以内**较为合理。

## 对 *IntelliJ IDEA* 的友好支持

- 配置了数据源的情况下，可以直接选中需要执行的sql右键，点击`Execute`执行sql，参数占位符(`:name`)和sql片段占位符(`${part}`)都会弹出输入框方便填写，直接进行测试sql
  ![](img/p.jpg)
  
  ![](img/p2.png)

## Example

### 初始化

```java
dataSource=new HikariDataSource();
dataSource.setJdbcUrl("jdbc:postgresql://127.0.0.1:5432/postgres");
dataSource.setUsername(...);
dataSource.setDriverClassName("org.postgresql.Driver");
XQLFileManager manager=new XQLFileManager(...);
BakiDao baki=new BakiDao(dataSource);
baki.setXqlFileManager(manager);
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
        .args(Args.create("id", 35))
        .collect(d -> d);
```

### 自定义分页查询

`/pgsql/data.sql`

```sql
/*[custom_paged]*/
select *
from test.region
where id > :id limit :limit
offset :offset;
```

```java
PagedResource<DataRow> res = baki.<DataRow>query("&pgsql.data.custom_paged", 1, 7)
                .count("select count(*) from <table> where id > :id")
                .args(Args.create("id", 8))
                .pageHelper(new PGPageHelper() {
                  // 重写此方法覆盖默认的分页构建逻辑，否则默认进行分页处理导致异常
                    @Override
                    public String pagedSql(String sql) {
                        return sql;
                    }
                }).collect(d -> d);
```

### 事务

事务设计为与线程绑定，使用请遵循事物的线程隔离性。

```java
Tx.using(()->{
  baki.update(...);
  baki.delete(...);
  baki.insert(...);
  ......
});
```

### 存储过程

```java
Tx.using(()->baki.call("{call test.fun_query(:c::refcursor)}",
        Args.of("c",Param.IN_OUT("result",OUTParamType.REF_CURSOR)))
        .<List<DataRow>>getFirstAs()
        .stream()
        .map(DataRow::toJson)
        .forEach(System.out::println));
```

