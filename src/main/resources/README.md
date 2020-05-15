# rabbit-jdbc 使用说明
## 关于
- 作者: chengyuxingo@gmail.com
- 对JDBC的一个薄封装工具类，提供基本的增删改查操作。
## 参数占位符说明
- `:name` (jdbc标准的传名参数写法，参数将被预编译安全处理)
- `${part}` (通用的字符串模版占位符，不进行预编译，用于动态sql的拼接)
- 字符串模版中还可以使用传名参数
## 外部SQL文件详解
- sql文件中可以包含任意符合标准的注释
- sql文件结尾以`.sql`结尾
- sql文件格式参考```data.sql.template```
- LightDao中如果配置了属性```setSqlPath```,则接口中需要写sql的所有方法都可以使用``&文件名.sql名``取地址符来获取sql文件中的sql
- sql文件名格式为``/*[name]*/``，sql文件中可以嵌套sql片段，使用`${片段名}`指定
- sql片段名格式化``/*{name}*/``，sql片段中可以嵌套sql片段，使用`${片段名}`指定
- sql文件将优先寻找sql文件内的sql片段
## 对 *IntelliJ IDEA* 的友好支持
- 配置了数据源的情况下，可以直接选中需要执行的sql右键，点击`Execute`执行sql，参数占位符(`:name`)和sql片段占位符(`${part}`)都会弹出输入框方便填写，直接进行测试sql
## 版本更新说明
### 1.3.1
1. 对ResultSet中返回结果类型 `Blob,Clob,Oracle时间类型` 等的处理；
2. 查询接口重构封装优化，批量插入重构优化;
3. 存储过程和函数执行优化，不再需要使用 `{}` 包裹;
4. `Filter`条件过滤器内部重构优化;
### 1.3.2
1. `PMode`枚举类中`INCLUDE`改为`IN_OUT`，语意表达更明确；
2. `Params`参数构建器中新增4个方法，`putIn,putOut,putInOut,putTemplate`，方便构建参数；
3. `DataRow` 中增加getType方法，用于方便的获取字段的java类型；
### 1.3.3
1. 为防止编码问题，日志输出改为英文;
2. SQL异常处理输出错误日志更明确;
### 1.3.4
1. 项目公用类解藕，引用模块rabbit-common
### 1.3.5
1. light接口移除事务接口，单独提取出事务处理类，统一通过getTransaction()来获取事务对象；
2. 事务增加*带有返回值类型*的自动提交回滚/事务:`using`方法
### 1.3.6
1. Light接口增加两个简单的分页查询方法
2. 增加简陋的多数据源事务同步- -；
3. LightSession设计为一个Connection作为一个会话事务独立，事务只对当前session有效；
4. LightDao设计为多线程情况下线程绑定多数据源事务同步，事务在当前线程跨数据源有效，适用于配置到Ioc中使用；
### 1.3.7
1.修复LightSession自动关闭Connection的Bug，改为手动管理
### 1.5.0
1. 增加`DataTable`类，用以提供建表操作；
2. 底层接口增加对`DataRow`的支持，促成一种通用的类型，来进行各种便利的操作，与`rabbit-excel`类库可使用`DataRow`进行便利的数据交互；
3. 增加库对库的数据传递基本接口，`transferTo(...)`；
4. 移除`LightSession`，由于可能在用户使用不当的情况下，事务提交或回滚将产生意料之外的结果，排除隐患BUG，直接用为多线程情况而设计的`LightDao`取而代之；
5. 包名和一些类重新整理
### 1.5.x
1. 修复condition不能以where以外其他操作符开头产生的空指针bug
2. 分页查询优化，基本接口简化
### 2.0.0
1. 对一些包进行重新整理
2. 移除`Conditions`，使用新的`Condition`作为替代
3. `Filter`重构优化
4. 新增类型 `ValueWrap` 值包装器，可以对参数进行一些额外的处理，可用于`Filter`中和`Param`入参中
5. `JdbcSupport` 释放连接提供抽象方法 `releaseConnection`, 可以自定义释放规则
6. 移除`DataTable`对象，移除建表操作，移除 `tableExists` 方法
7. 优化打印SQL执行日志和参数，更直观
8. 修复 `SqlFileManager` 一直占用文件流，IDEA编辑sql文件后，导致IDEA热部署失败的BUG
9. 修复同步事务管理器提交事务后连接不释放的BUG
10. `light`接口中移除`getTransaction`方法
11. 废弃 `SyncTransactionManager`，由 `Transaction` 取而代之
12. 事务管理重新优化，并与`light`接口分离解藕
13. 增加条件过滤器通用接口`IFilter`，可自定义过滤器在`Condition`中使用
14. sql查询结果集解析优化，由以前的直接获取单元格类型（在某些时候无法正确获取类型而显示`java.lang.Object`，包括解析`Blob`字段后应为`[B`类型，然而`types`对应的列还显示为`Blob`造成显示错误）改为获取值类型，现在`DataRow`的`types`属性可以准确的获取到每一列值的类型名称
### 2.0.1
1. 几乎修复解析sql占位符的bug，现在sql字符串可以形如：
```"select t.id || 'number' || 'name:cyx', '{\"name\":\"user\"}'::jsonb from test.user where id = :id::integer and id > :idc and name=text :username"```也能正确解析
### 2.0.2
1. 包名`impl`重新命名为`dao`
2. 增加`IOutParam`接口，提供函数或存储过程出参类型的通用接口，如默认实现`OUTParamType`无法满足的情况下，可自行实现此接口
3. 修复sql文件内，注释在第一行的解析错误bug
### 2.0.3
1. `SQLFileManager`解析sql文件优化
2. 事务名`Transaction`改为`Tx`
### 2.0.4
1. sql异常日志打印调整输出更明确
### 2.0.5
1. `SqlFileManager` 解析sql片段提高安全性，多个sql文件包含同名文件支持，修复开头有多行注释解析失败的bug
### 2.0.6
1. 分页查询优化
2. `OraclePageHelper` 和 `PGPageHelper` 改为可被继承
3. 小提示：`PostgreSQL`中，带有问号的操作符`(?,?|,?&,@?)`可以使用双问号`(??,??|,??&,@??)`解决预编译sql参数未设定的报错()，或者直接使用函数