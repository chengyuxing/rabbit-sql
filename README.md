# rabbit-sql 使用说明

- 对JDBC的一个薄封装工具类，提供基本的增删改查操作。

- 在最久远的版本中实际上做过实体解析映射sql，实现过jpa的一部分，至于后来为什么移除了，其中一个原因是实体如果用不好，修改表结构很麻烦，有风险，再者，对于关系数据库，实体映射生成的sql几乎都是简单的标准sql，这并不能作为跨数据库的亮点，该手写的还是得手写，干脆以最简单为目标，其他交给大家去扩展好了。

- Scala并不兼容Java的实体类，希望尽量能在Scala语言中用起来也不那么麻烦。

- 此库以追求简单稳定高效为目标，做到几乎不使用反射，不支持查询结果实体映射，返回对象类型统一为[`DataRow`](https://github.com/chengyuxing/rabbit-common/blob/master/src/main/java/rabbit/common/types/DataRow.java)，若需要实体映射，可自行轻松通过[`DataRow`](https://github.com/chengyuxing/rabbit-common/blob/master/src/main/java/rabbit/common/types/DataRow.java)来实现。

- 此库核心非ORM，而是最大限度的接近以原生JDBC的性能执行sql，高度自由化的定制优化sql，调试sql一气呵成。

- maven dependency (jdk1.8)

  ```xml
  <dependency>
      <groupId>com.github.chengyuxing</groupId>
      <artifactId>rabbit-sql</artifactId>
      <version>4.3.2</version>
  </dependency>
  ```
## 参数占位符说明
- `:name` (jdbc标准的传名参数写法，参数将被预编译安全处理)
- `${part}` (通用的字符串模版占位符，不进行预编译，用于动态sql的拼接)
- 字符串模版中还可以使用传名参数
## 外部SQL文件详解
- sql文件中可以包含任意符合标准的注释
- sql文件结尾以`.sql`结尾
- sql文件格式参考```data.sql.template```
- BakiDao中如果配置了属性```setSqlFileManager```,则接口中需要写sql的所有方法都可以使用``&文件名.sql名``取地址符来获取sql文件中的sql
- sql文件名格式为``/*[name]*/``，sql文件中可以嵌套sql片段，使用`${片段名}`指定
- sql片段名格式化``/*{name}*/``，sql片段中可以嵌套sql片段，使用`${片段名}`指定
- sql文件将优先寻找sql文件内的sql片段
## 动态SQL

- 结构类似于Mybatis的if标签，--#if和--#fi必须成对出现

- 支持的运算符：

  | 运算符 | 说明           |
  | ------ | -------------- |
  | <      | 大于           |
  | >      | 小于           |
  | >=     | 大于等于       |
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
--#if :age !=null
t.age > 21
--#fi
--#if :name != null
and t.name ~ :name
--#fi
--#if :age <> blank && :age < 90
and age < 90
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
dataSource = new HikariDataSource();
dataSource.setJdbcUrl("jdbc:postgresql://127.0.0.1:5432/postgres");
dataSource.setUsername("chengyuxing");
dataSource.setDriverClassName("org.postgresql.Driver");

SQLFileManager manager = new SQLFileManager("pgsql/data.sql", "pgsql/other.sql");

BakiDao baki = new BakiDao(dataSource);
baki.setSqlFileManager(manager);
```

### 查询

```java
try (Stream<DataRow> fruits = baki.query("select * from fruit")) {
            fruits.limit(10).forEach(System.out::println);
        }
```

### 分页查询
```java
PagedResource<DataRow> res = baki.<DataRow>query("&pgsql.data.select_user", 1, 10)
                .args(Args.create().set("id", 35))
                .collect(d -> d);
```

### 存储过程

```java
Tx.using(() -> baki.call("{call test.fun_query(:c::refcursor)}",
                Args.of("c", Param.IN_OUT("result", OUTParamType.REF_CURSOR)))
                .<List<DataRow>>get(0)
                .stream()
                .map(DataRow::toMap)
                .forEach(System.out::println));
```

