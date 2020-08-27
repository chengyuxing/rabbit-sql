# rabbit-sql 使用说明
- 对JDBC的一个薄封装工具类，提供基本的增删改查操作。
## 参数占位符说明
- `:name` (jdbc标准的传名参数写法，参数将被预编译安全处理)
- `${part}` (通用的字符串模版占位符，不进行预编译，用于动态sql的拼接)
- 字符串模版中还可以使用传名参数
## 外部SQL文件详解
- sql文件中可以包含任意符合标准的注释
- sql文件结尾以`.sql`结尾
- sql文件格式参考```data.sql.template```
- LightDao中如果配置了属性```setSqlFileManager```,则接口中需要写sql的所有方法都可以使用``&文件名.sql名``取地址符来获取sql文件中的sql
- sql文件名格式为``/*[name]*/``，sql文件中可以嵌套sql片段，使用`${片段名}`指定
- sql片段名格式化``/*{name}*/``，sql片段中可以嵌套sql片段，使用`${片段名}`指定
- sql文件将优先寻找sql文件内的sql片段
## 对 *IntelliJ IDEA* 的友好支持
- 配置了数据源的情况下，可以直接选中需要执行的sql右键，点击`Execute`执行sql，参数占位符(`:name`)和sql片段占位符(`${part}`)都会弹出输入框方便填写，直接进行测试sql
![](img/p.jpg)
![](img/p2.png)
## Excaple

### 初始化

```java
dataSource = new HikariDataSource();
dataSource.setJdbcUrl("jdbc:postgresql://127.0.0.1:5432/postgres");
dataSource.setUsername("chengyuxing");
dataSource.setDriverClassName("org.postgresql.Driver");

SQLFileManager manager = new SQLFileManager("pgsql/data.sql", "pgsql/other.sql");

LightDao light = new LightDao(dataSource);
light.setSqlFileManager(manager);
```

### query

```java
try (Stream<DataRow> fruits = orclLight.query("select * from fruit")) {
            fruits.limit(10).forEach(System.out::println);
        }
```

### call Function

```java
List<DataRow> rows = Tx.using(() -> {
  DataRow row = light.function("call test.fun_query(:c::refcursor)",
                           Params.builder()
                           .put("c", Param.IN_OUT("result", OUTParamType.REF_CURSOR))
                           .build());
  System.out.println(row);
  return row.get(0);
});
rows.forEach(System.out::println);
```

