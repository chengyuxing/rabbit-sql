# rabbit-sql 动态 SQL 与 XQL 文件管理器使用指南

> 本文基于 [rabbit-sql.com](https://rabbit-sql.com) 官网上「动态 SQL」与「XQL 文件管理器」相关文档，并结合 `rabbit-sql`、`rabbit-common` 两个项目的源码实现以及项目中的 `.xql` 示例文件整理而成。
>
> 目标：把 XQL 文件如何组织、动态 SQL 如何解析、最终 SQL 如何生成讲清楚，形成一份可直接用于团队开发和排障的说明。

## 目录

1. [整体认识](#1-整体认识)
2. [XQL 文件管理器](#2-xql-文件管理器)
3. [动态 SQL 概述与书写规则](#3-动态-sql-概述与书写规则)
4. [表达式脚本](#4-表达式脚本)
5. [控制指令详解](#5-控制指令详解)
6. [模板、变量与预编译机制](#6-模板变量与预编译机制)
7. [典型实战示例](#7-典型实战示例)
8. [常见错误与排查建议](#8-常见错误与排查建议)
9. [与官方源码模块的对应关系](#9-与官方源码模块的对应关系)

---

## 1. 整体认识

rabbit-sql 中的动态 SQL 不是一个独立存在的“模板标签库”，而是建立在 **XQL 文件管理器（XQLFileManager）** 之上：

```text
XQL 文件
   ↓
XQLFileManager 扫描阶段
   ├── 解析出 SQL Object
   ├── 解析出模板片段
   └── 解析出 SQL 元数据
   ↓
运行时调用
   ↓
动态 SQL 引擎（RabbitScriptEngine）
   ↓
最终 SQL
   ↓
SqlGenerator 将命名参数编译为 ?
```

它的设计原则是：

- SQL 仍然是第一公民，不被 XML 或字符串拼接替代；
- 所有扩展都基于 SQL 注释实现；
- 普通 SQL 文件仍然可以被主流数据库工具打开、高亮和做语法检查；
- 动态能力在执行阶段才介入，文件扫描阶段只做结构解析。

主要源码位置：

- `com.github.chengyuxing.sql.XQLFileManager`：XQL 文件扫描、SQL 对象解析、模板合并、元数据解析；
- `com.github.chengyuxing.common.script.RabbitScriptEngine`：动态 SQL 脚本编译与执行；
- `com.github.chengyuxing.common.script.ast.impl.RabbitScriptParser`：动态 SQL 语法解析；
- `com.github.chengyuxing.common.script.ast.impl.RabbitScriptEvaluator`：动态 SQL 执行求值；
- `com.github.chengyuxing.sql.util.SqlGenerator`：命名参数到 `?` 的预编译转换。

---

## 2. XQL 文件管理器

### 2.1 XQL 文件管理器是什么

XQL File Manager 是 rabbit-sql 提供的 SQL 资源管理与解析组件，作用是在保持原生 SQL 语义不变的前提下，为 SQL 文件提供以下能力：

- 动态 SQL 脚本；
- SQL 片段复用；
- SQL 元数据定义；
- 多文件统一管理。

支持的文件类型：

- `.xql`：推荐类型，IDEA 插件能提供增强支持；
- `.sql`：同样可以被解析。

> 官网明确说明：IDEA 插件目前主要识别 `.xql` 文件，因此团队开发时建议统一使用 `.xql`。

### 2.2 配置方式

XQL File Manager 支持 YAML 和 properties 两种配置。推荐在 classpath 下放置：

```text
src/main/resources/xql-file-manager.yml
```

一个完整的配置示例：

```yaml
constants:
  db: pgsql

files:
  user: xqls/user.xql
  order: xqls/order.xql
  remote: http://127.0.0.1:8080/share/cyx.xql?token=${env.TOKEN}

pipes:
  isIdCard: org.example.IsIdCard

charset: UTF-8
named-param-prefix: ':'
```

配置项说明：

| 配置 | 作用 |
| --- | --- |
| `files` | 注册需要解析的 SQL 文件，key 为 alias，value 为文件路径 |
| `constants` | 全局常量，可在 SQL 中用 `${name}` 引用 |
| `pipes` | 注册自定义管道，value 为实现类全限定名 |
| `charset` | 解析文件编码，默认 UTF-8 |
| `named-param-prefix` | 预编译命名参数前缀，默认 `:` |

`files` 支持多种协议：

- `classpath`：默认，例如 `xqls/user.xql`；
- `file://`；
- `ftp://`；
- `http://`、`https://`。

YAML 还支持以下增强写法：

```yaml
constants:
  base: &basePath pgsql

files:
  foo: !path [ *basePath, foo.xql ]
  bar: bar.xql
  remote: http://127.0.0.1:8080/share/cyx.xql?token=${env.TOKEN}
```

- `!path`：使用 `/` 拼接数组为路径；
- `!join`：直接拼接数组为字符串；
- `${env.xxx}`：读取系统环境变量。

Spring Boot Starter 中也支持在 `application.yml` 中直接配置：

```yaml
baki:
  xql-file-manager:
    files:
      user: xqls/user.xql
      order: xqls/order.xql
    constants:
      db: postgresql
    named-param-prefix: ':'
```

如果希望插件直接识别和管理，更推荐单独使用 classpath 下的 `xql-file-manager.yml`。

### 2.3 XQL 文件结构

#### 文件描述

文件顶部可以写注释。注释中如果包含 `@@@` 区域，这部分内容会作为文件描述信息：

```sql
/*
 Created by IntelliJ IDEA.
 User: someone
 @@@
 本文件用于用户模块查询。
 所有 SQL 都以 user_ 前缀命名。
 @@@
*/
```

#### SQL 对象

一个 XQL 文件由多个 SQL 对象组成，SQL 对象之间使用 `;` 分隔。`;` 是解析阶段的核心边界。

最基本的 SQL 对象写法：

```sql
/*[queryUserById]*/
select *
from test.user
where id = :id;
```

SQL 名称必须写在 `/*[name]*/` 中。

#### 模板片段

模板片段使用 `/*{name}*/` 定义，可以供其他 SQL 通过 `${name}` 引用。

例如：

```sql
/*{where}*/
where id = :id ${order};

/*[queryUser]*/
select * from users ${where};
```

#### SQL 描述

SQL 对象可以有自己的描述，使用 `/*#description#*/`：

```sql
/*#根据 ID 查询用户#*/
/*[queryUserById]*/
select * from test.user where id = :id;
```

#### 元数据

元数据使用 `-- @key value` 形式，必须位于 SQL 对象的首部，并且连续出现：

```sql
/*[queryUsers]*/
-- @cache 30m
-- @rules admin,guest
select * from users;
```

元数据不参与动态 SQL，也不影响最终执行结果，但可以被缓存、拦截器等组件读取。

### 2.4 SQL 对象的完整结构

官网文档中的典型结构：

```sql
/*[queryGuests]*/
/*#查询访客#*/
-- @cache 30m
-- @rules admin,guest
-- #check :age > 30 throw '年龄不能大于30岁'
-- #var id = 14
-- #var users = 'a,xxx,c' | split(',')
select * from test.guest where
-- //TEMPLATE-BEGIN:myCnd
    id = :id
    and name in (
        -- #for item of :users; last as isLast
            -- #if !:isLast
            :item,
            -- #else
            :item
            -- #fi
        -- #done
    )
-- //TEMPLATE-END
;
```

一个 SQL 对象由以下部分组成：

| 部分 | 形式 | 是否必需 |
| --- | --- | --- |
| 名称 | `/*[name]*/` | 必需 |
| 描述 | `/*#desc#*/` | 可选 |
| 元数据 | `-- @key value` | 可选 |
| 函数体 | SQL + 动态脚本 + 模板 | 必需 |

### 2.5 扫描阶段与调用阶段

XQL 文件中的每个 SQL 对象有两个相互独立的阶段：

1. **文件扫描阶段**  
   解析元数据、提取模板、合并内联模板、构建 SQL 结构模型。

2. **SQL 调用阶段**  
   执行动态 SQL 脚本，生成最终可执行 SQL。

这意味着：

- 模板合并发生在扫描阶段；
- `#if`、`#for`、`#check` 等控制指令发生在调用阶段；
- 同一个 SQL 对象可以被多次调用，每次根据参数不同生成不同 SQL。

### 2.6 独立模板

独立模板适合复用多个 SQL 中相同的完整片段：

```sql
/*{orderById}*/
order by id desc;

/*[queryUsers]*/
select * from users ${orderById};
```

注意：独立模板如果是 `where id = :id` 这类不完整 SQL，在 SQL IDE 中会显示语法错误或高亮异常，这是独立模板的天然局限。

### 2.7 内联模板

内联模板直接在某个 SQL 对象内部标记一块区域，使其可以被同文件中的其他 SQL 引用：

```sql
-- //TEMPLATE-BEGIN:myCnd
...
-- //TEMPLATE-END
```

示例：

```sql
/*[queryList]*/
select t.id, t.name
from guest t
where
-- //TEMPLATE-BEGIN:queryListCnd
    1 = 1
    -- #if :id != blank
    and t.id = :id
    -- #fi
    -- #if :name != blank
    and t.name = :name
    -- #fi
-- //TEMPLATE-END
;

/*[queryCount]*/
select count(*)
from guest t
where ${queryListCnd};
```

内联模板的特点：

- 不破坏所在 SQL 对象的完整性；
- 避免单独 `where` 片段造成 IDE 语法检查误报；
- 内联模板内部仍然可以写动态 SQL；
- 不允许嵌套内联模板。

官方建议：

- 模板内部第一个连接条件可以保留在模板外，例如把 `and` 写在引用处；
- 多个模板命名建议以所在 SQL 名称开头，例如 `queryUsersFilter`，避免大量模板时混淆。

### 2.8 模板合并与递归引用

XQL 文件扫描完成后会执行模板合并：

```text
SQL 中存在 ${name}
  → 先从本文件模板集合中查找
  → 再从全局 constants 中查找
  → 递归替换，直到没有 ${...}
```

模板可以递归引用其他模板：

```sql
/*{fields}*/
id, name, ${otherFields};

/*{otherFields}*/
age, address;

/*[getUser]*/
select ${fields}
from test.user
where id = :id;
```

最终：

```sql
select id, name, age, address
from test.user
where id = :id
```

### 2.9 多语句块

当 SQL 是 PL/SQL 或 DDL 时，内部可能包含多个 `;`。为避免解析器过早截断，可以在内部 `;` 后追加行注释 `--`：

```sql
/*[myPlsql]*/
begin; --
  select 1; -- 一些描述
  select 2; --
end;
```

### 2.10 调用 XQL 中的 SQL

配置好 XQL File Manager 后，可以通过以下形式调用：

```text
&<alias>.<sqlName>
```

例如文件 alias 为 `user`，SQL 名为 `queryUserById`：

```java
baki.query("&user.queryUserById")
    .arg("id", 100)
    .stream();
```

源码中 `BakiDao#prepareSql` 会识别以 `&` 开头的 SQL，并：

1. 取出 alias 和 sqlName；
2. 从 `XQLFileManager` 获取 SQL 对象；
3. 执行动态 SQL 引擎；
4. 对最终 SQL 做 `${}` 模板替换；
5. 通过 `SqlGenerator` 将命名参数转换为 `?`。

SQL 引用还支持修饰符，例如：

```text
&user.queryAll^page
&user.queryAll^count
```

`^page`、`^count` 用于分页场景下的内部处理。

---

## 3. 动态 SQL 概述与书写规则

### 3.1 动态 SQL 如何工作

动态 SQL 依赖 XQL File Manager。它不是在 SQL 外层包裹 XML 标签，而是利用 SQL 行注释承载控制指令：

```sql
select * from test.user
where 1 = 1
-- #if :name != blank
  and name = :name
-- #fi
;
```

调用时传入：

```json
{ "name": "cyx" }
```

最终得到：

```sql
select * from test.user
where 1 = 1
  and name = :name
```

随后 `:name` 会被预编译为 `?`，值为 `cyx`。

### 3.2 注释标记

动态 SQL 指令推荐写成：

```sql
-- #if :name != blank
```

也可以写成：

```sql
#if :name != blank
```

原因是动态 SQL 引擎会先识别以 `#` 开头的指令行。但如果数据库不是 MySQL，裸 `#` 可能不是合法 SQL 注释，因此：

- 标准写法：`-- #if`；
- 如果是 MySQL，并且你能接受 `#` 作为 MySQL 注释，也可以直接写 `#if`。

每个控制指令关键字都以 `#` 开头，并且必须单独占一行。指令名称不区分大小写，例如 `#IF`、`#if` 都能识别。

### 3.3 指令必须成对出现

所有块级指令都有明确的开始和结束：

| 开始 | 结束 |
| --- | --- |
| `#if` | `#fi` |
| `#guard` | `#throw` |
| `#switch` | `#end` |
| `#choose` | `#end` |
| `#for` | `#done` |

块级指令可以嵌套，例如 `#if` 内再写 `#if`、`#choose`、`#for` 等。

### 3.4 SQL 参数与控制指令变量是两个概念

虽然都以 `:` 开头，但含义不同：

| 位置 | 含义 | 最终结果 |
| --- | --- | --- |
| SQL 语句中 `:id` | 命名参数 | 被编译为 `?`，作为 SQL 参数执行 |
| 控制指令中 `:id` | 值传递 | 仅用于解析指令，参与条件判断 |

例如：

```sql
-- #if :id != blank
  and id = :id
-- #fi
```

- `#if :id != blank` 中的 `:id` 用于判断；
- `id = :id` 中的 `:id` 是预编译参数。

### 3.5 动态 SQL 支持的关键字

动态 SQL 内置指令完整列表：

- `#check`
- `#var`
- `#if`、`#else`、`#fi`
- `#guard`、`#throw`
- `#switch`、`#case`、`#default`、`#break`、`#end`
- `#choose`、`#when`、`#default`、`#break`、`#end`
- `#for`、`#done`

---

## 4. 表达式脚本

### 4.1 变量表达式

控制指令中的变量以 `:` 开头，并支持路径访问：

```text
:id
:user.name
:users[0]
:map['key']
:books[1].title
```

底层由 `KeyExpressionParser` 解析，支持：

- 点号访问：`user.name`
- 数组或 map 下标访问：`users[0]`、`map['key']`

### 4.2 常量值

常量值分为加引号和不加引号：

```text
'abc'
"abc"
abc
12
3.14
-1
true
false
null
blank
```

规则：

- 数字不加引号时是数字；
- 非数字、非关键字的裸标识符默认为字符串；
- `'blank'` 是字符串，`blank` 是内置关键字；
- `'12'` 是字符串，`12` 是数字。

示例：

```sql
-- #if :name = cyx
```

等价于：

```sql
-- #if :name = 'cyx'
```

但 `blank` 不等于 `'blank'`。

### 4.3 内置常量

| 常量 | 含义 |
| --- | --- |
| `null` | null |
| `true` | 布尔真 |
| `false` | 布尔假 |
| `blank` | null、空字符串、空数组、空集合 |

`blank` 是最常用的判断条件，例如：

```sql
-- #if :ids != blank
```

### 4.4 单目表达式

当表达式只写一个值时，会进行“是否非 blank”的判断：

```sql
-- #if :ids
```

等价于：

```sql
-- #if :ids != blank
```

逻辑取反：

```sql
-- #if !:ids
```

等价于：

```sql
-- #if :ids == blank
```

如果值本身是布尔值，则直接使用布尔值。

### 4.5 比较运算符

| 运算符 | 说明 |
| --- | --- |
| `<` | 小于 |
| `>` | 大于 |
| `>=` | 大于等于 |
| `<=` | 小于等于 |
| `==`、`=` | 等于 |
| `!=`、`<>` | 不等于 |
| `~` | 正则包含 |
| `!~` | 正则不包含 |
| `@` | 正则匹配 |
| `!@` | 正则不匹配 |

说明：

- `<`、`>`、`>=`、`<=` 是数值比较，要求两边都是数字或数字字符串；
- `=`、`==` 使用字符串化比较；
- `~` 使用 `Matcher.find()`，表示包含匹配；
- `@` 使用 `Matcher.matches()`，表示完整匹配。

### 4.6 逻辑运算与括号

支持：

- `&&`：逻辑与；
- `||`：逻辑或；
- `!`：逻辑非；
- `()`：嵌套括号。

示例：

```sql
-- #if !(:id >= 0 || :name | length <= 3) && :age > 21
```

### 4.7 管道

管道语法类似 shell：

```text
:id | upper | length
```

数据从左到右依次经过管道处理：

```text
abc → upper → ABC → length → 3
```

内置管道：

| 管道 | 作用 | 示例 |
| --- | --- | --- |
| `length` | 获取数组、集合或字符串长度 | `:name \| length` |
| `upper` | 转大写 | `:name \| upper` |
| `lower` | 转小写 | `:name \| lower` |
| `kv` | Map 或 Java 对象转为键值对集合 | `:sets \| kv` |
| `nvl` | 值为 null 时返回默认值 | `:name \| nvl('guest')` |
| `split` | 按分隔符拆分字符串 | `:users \| split(',')` |
| `in` | 判断值是否在参数列表中 | `:status \| in('a','b','c')` |

管道可以带参数：

```sql
-- #var list = 'cyx,jack,mike' | split(',')
-- #switch :name | length
-- #case 3
```

也可以链式使用：

```sql
-- #if :name | upper | length <= 3
```

### 4.8 自定义管道

实现接口：

```java
com.github.chengyuxing.common.script.pipe.IPipe
```

例如：

```java
package org.example;

import com.github.chengyuxing.common.script.pipe.IPipe;

public class IsIdCard implements IPipe<Boolean> {
    @Override
    public Boolean transform(Object value, Object... params) {
        return value != null && value.toString().matches("\\d{17}[\\dXx]");
    }
}
```

然后在配置中注册：

```yaml
pipes:
  isIdCard: org.example.IsIdCard
```

即可在动态 SQL 中使用：

```sql
-- #if :idCard | isIdCard
```

---

## 5. 控制指令详解

### 5.1 `#check`：参数断言

作用：

在 SQL 真正到达数据库之前，对参数做一次合法性验证。如果条件成立，则抛出 `CheckViolationException`，并终止后续操作。

语法：

```text
#check <表达式> throw '<错误信息>'
```

示例：

```sql
-- #check :id == null throw 'ID不能为null'
-- #check :age > 30 throw '年龄不能大于30岁'
select * from test.user where id = :id;
```

使用价值：

- 避免拿到数据库连接后才发现参数类型错误；
- 同一条 SQL 被多个入口调用时，校验逻辑集中在 SQL 中，不容易遗漏；
- 让 Java 业务代码更聚焦业务，而不是重复参数校验。

### 5.2 `#var`：变量定义

作用：

定义动态 SQL 内部的临时变量。变量值可以是常量，也可以是经过管道处理后的输入参数。

语法：

```text
#var <变量名> = <常量 | :参数> [| pipe1 | pipe2 | ...]
```

示例：

```sql
-- #var list = 'cyx,jack,mike' | split(',')
-- #var newId = :id
-- #var safeAge = :age
select *
from test.user
where id = :newId
  and age < :safeAge;
```

解释：

- `#var list` 把字符串拆分成了数组；
- `#var newId = :id` 把输入参数赋给内部变量；
- 定义的变量可以在 SQL 中作为命名参数使用，也可以在后续控制指令中使用。

注意：

- 变量名不要与输入参数同名；
- `#var` 定义在当前作用域内有效，例如在 `#for` 内定义，则只在循环体作用域内可见。

### 5.3 `#if`、`#else`、`#fi`

作用：

条件判断，和编程语言中的 `if` 类似。

语法：

```text
#if <表达式>
    ...
#else
    ...
#fi
```

其中 `#else` 可选。没有 `else if`，需要多条件分支时建议使用 `#choose`。

示例：

```sql
select *
from test.user
where 1 = 1
-- #if :id != blank
  and id = :id
-- #else
  and enabled = true
-- #fi
;
```

嵌套示例：

```sql
-- #if :name != blank
  and name = :name
  -- #if :age > 0
    and age = :age
  -- #fi
-- #fi
```

### 5.4 `#guard`、`#throw`

作用：

守卫语句。如果条件满足，执行 `#guard` 和 `#throw` 之间的 SQL；否则执行 `#throw`，抛出 `GuardViolationException`。

可以理解为 `#check` + `#if` 的组合：

- 需要动态拼接 SQL；
- 同时需要校验参数合法性。

语法：

```text
#guard <表达式>
    ...
#throw '<错误信息>'
```

示例：

```sql
select * from test.guest
where
-- #guard :id != blank
    id = :id
-- #throw 'ID is required!'
```

如果 `:id` 非 blank，最终生成：

```sql
select * from test.guest
where
    id = :id
```

如果 `:id` 为 blank，则抛出异常，不生成 SQL。

### 5.5 `#switch`、`#case`、`#default`、`#break`、`#end`

作用：

类似编程语言中的 `switch`，按顺序匹配每个 `#case` 分支。第一个匹配成功后执行对应分支，并立即退出整个 switch。

语法：

```text
#switch <值> [| pipe1 | pipe2 | ...]
    #case <值1>, <值2>, <值N>
        ...
    #break
    #case <值>
        ...
    #break
    #default
        ...
    #break
#end
```

示例：

```sql
select *
from test.user
where id = :id
-- #switch :name
    -- #case cyx, mike, 'bob'
    and t.name = :name
    -- #break
    -- #case 'guest'
    and t.name = 'guest'
    -- #break
    -- #default
    and t.name is null
    -- #break
-- #end
```

多个值写在一个 `#case` 中，用逗号分隔。裸值如果不是纯数字或 `null`、`blank`、`true`、`false`，默认按字符串处理。

`#switch` 的值也可以经过管道：

```sql
-- #switch :name | length
    -- #case 3
    ...
    -- #break
    -- #case 4
    ...
    -- #break
-- #end
```

### 5.6 `#choose`、`#when`、`#default`、`#break`、`#end`

作用：

类似 `if-else if-else`，按顺序匹配每个 `#when` 表达式。第一个为 true 的分支执行后，直接退出整个 choose。

语法：

```text
#choose
    #when <表达式1>
        ...
    #break
    #when <表达式2>
        ...
    #break
    #default
        ...
    #break
#end
```

示例：

```sql
update test.user
set
-- #choose
    -- #when :age < 100
    age = :age,
    -- #break
    -- #when :age > 100
    age = 100,
    -- #break
    -- #default
    age = 101,
    -- #break
-- #end
where id = 10;
```

`#choose` 与 `#switch` 的区别：

- `#switch` 使用等值匹配；
- `#choose` 使用完整表达式条件。

### 5.7 `#for`、`#done`

作用：

遍历集合，并把循环体内的内容累加到最终 SQL 中。最常用于生成 `in (?, ?, ?)` 这类预编译 SQL。

语法：

```text
#for item of :list [| pipe1 | pipeN | ...] [;index as i] [;first as isFirst] [;last as isLast] [;odd as isOdd] [;even as isEven]
    ...
#done
```

完整 for 表达式：

```text
item of :list [| pipe1 | pipeN | ... ] [;index as i] [;last as isLast] ...
```

关键字：

- `of`
- `as`

说明：

| 部分 | 含义 |
| --- | --- |
| `item` | 当前迭代值变量名 |
| `:list` | 被迭代对象，可以是集合、数组等 |
| `| pipe` | 可选，迭代前先经过管道处理 |
| `index` | 当前索引 |
| `first` | 是否为第一个元素 |
| `last` | 是否为最后一个元素 |
| `odd` | 当前索引是否为奇数 |
| `even` | 当前索引是否为偶数 |

示例：

```sql
select *
from test.user
where id = :id
or name in (
-- #for name of :names; last as isLast
    -- #if !:isLast
    :name,
    -- #else
    :name
    -- #fi
-- #done
)
```

如果 `names = ["cyx", "mike", "bob"]`，最终生成：

```sql
select *
from test.user
where id = :id
or name in (
    ?
    ,
    ?
    ,
    ?
)
```

实际内部会生成三个命名参数并绑定对应值。

---

## 6. 模板、变量与预编译机制

### 6.1 `${name}` 字符串模板

`${name}` 是字符串模板占位符，不进行预编译。它用于 SQL 片段复用和常量替换。

两种格式：

| 写法 | 行为 |
| --- | --- |
| `${name}` | 值直接展开，字符串不加引号 |
| `${!name}` | 值展开，并对字符串做安全引号处理 |

示例：

```sql
select ${fields}
from test.user
where name in (${!names});
```

参数：

```java
Map<String, Object> args = new HashMap<>();
args.put("fields", "id, name, age");
args.put("names", Arrays.asList("cyx", "mike", "bob"));
```

最终：

```sql
select id, name, age
from test.user
where name in ('cyx', 'mike', 'bob')
```

`${!name}` 对集合会做单引号转义，例如 `I'm OK!` 会变成 `'I''m OK!'`。

### 6.2 `${name}` 与 `#for` 的区别

`${!names}` 能快速生成 `in ('a', 'b')`，但它不是预编译参数。

```sql
-- 直接字符串替换
where name in (${!names})
```

`#for` 更适合需要预编译、需要保证所有值都走 JDBC 参数绑定的场景：

```sql
where name in (
-- #for name of :names; last as isLast
    :name
    -- #if !:isLast
    ,
    -- #fi
-- #done
)
```

推荐原则：

- 只需要拼 SQL 片段、字段名、排序条件：使用 `${...}`；
- 需要安全绑定用户输入、构造 `in`、`update set`：使用 `:name` 命名参数和 `#for`。

### 6.3 循环内命名参数的生成机制

在 `#for` 循环体内写 `:item` 时，框架并不是直接保留 `:item`，而是为每次循环生成唯一变量名。

例如：

```sql
-- #for item of :users; last as isLast
    :item
-- #done
```

内部近似过程：

```text
:_var.item_0
,_var.item_1
,_var.item_2
```

同时在参数集合中放入：

```text
_var.item_0 = users[0]
_var.item_1 = users[1]
_var.item_2 = users[2]
```

最终由 `SqlGenerator` 把每个 `:_var.item_0` 编译为 `?`。

因此：

- `_var` 是内部变量前缀，业务 SQL 中不要手动依赖它；
- 循环体内可以使用 `:item`、`:item.field`、`:item['field']` 等；
- `${item.field}` 也受循环作用域支持。

### 6.4 内置变量 `_databaseId`

动态 SQL 中可以使用内置变量：

```text
:_databaseId
:_databaseId.name
```

其值为当前数据库信息对象 `com.github.chengyuxing.sql.types.DatabaseInfo`。

示例：

```sql
select * from test.user
where id = 3
-- #if :_databaseId.name == 'postgresql'
   or id = 9
-- #fi
-- #if :_databaseId.name == 'oracle'
   or id = 10
-- #fi
;
```

这个变量由 `BakiDao` 在执行动态 SQL 时自动注入，不需要用户传入。

### 6.5 `#var` 与命名参数的协作

示例：

```sql
-- #var id = 14
-- #var users = 'a,xxx,c' | split(',')
select *
from test.guest
where id = :id
  and name in (
    -- #for item of :users; last as isLast
        :item
        -- #if !:isLast
        ,
        -- #fi
    -- #done
  );
```

执行逻辑：

1. `#var id = 14` 绑定内部变量 `id`；
2. `#var users` 把字符串拆分成数组；
3. SQL 中 `:id` 因为与内部变量 `id` 同名，被替换为内部变量对应的命名参数；
4. `#for` 生成多个内部命名参数；
5. 最终全部编译为 `?`。

---

## 7. 典型实战示例

### 7.1 根据可选条件查询用户

```sql
/*[queryUsers]*/
select *
from test.user
where 1 = 1
-- #if :id != blank
  and id = :id
-- #fi
-- #if :name != blank
  and name = :name
-- #fi
-- #if :status | in('active','disabled')
  and status = :status
-- #fi
order by id desc;
```

调用：

```java
baki.query("&user.queryUsers")
    .arg("name", "cyx")
    .stream();
```

### 7.2 使用 `#for` 构造预编译 `in`

```sql
/*[queryByIds]*/
select *
from test.user
where id in (
    -- #for id of :ids; last as isLast
        :id
        -- #if !:isLast
        ,
        -- #fi
    -- #done
);
```

参数：

```java
Args.of("ids", Arrays.asList(1, 2, 3, 4, 5))
```

最终执行 SQL：

```sql
select *
from test.user
where id in (?, ?, ?, ?, ?)
```

### 7.3 使用 `#for` 和 `kv` 动态更新字段

```sql
/*[update]*/
update test.user
set
-- #for set of :sets | kv; last as isLast
    ${set.key} = :set.value
    -- #if !:isLast
    ,
    -- #fi
-- #done
where id = :id;
```

参数：

```json
{
  "id": 10,
  "sets": {
    "name": "abc",
    "age": 30,
    "address": "kunming"
  }
}
```

解释：

- `:sets` 是 Map；
- `kv` 管道将其转换成 `List<KeyValue>`；
- `set.key` 作为字段名，使用 `${set.key}` 直接替换；
- `set.value` 作为参数值，使用 `:set.value` 预编译绑定。

### 7.4 多条件组合与内联模板

列表查询：

```sql
/*[queryList]*/
select t.id, t.name
from guest t
where
-- //TEMPLATE-BEGIN:queryListCnd
    1 = 1
    -- #if :id != blank
    and t.id = :id
    -- #fi
    -- #if :keyword != blank
    and t.name like :keyword
    -- #fi
-- //TEMPLATE-END
;
```

统计查询复用相同条件：

```sql
/*[queryCount]*/
select count(*)
from guest t
where ${queryListCnd};
```

### 7.5 根据不同数据库生成不同 SQL

```sql
/*[queryByDb]*/
select *
from test.user
where id = 3
-- #if :_databaseId.name == 'postgresql'
  or id = 9
-- #fi
-- #if :_databaseId.name == 'oracle'
  or id = 10
-- #fi
;
```

### 7.6 使用 `#choose` 模拟复杂 if-else

```sql
/*[chooseExample]*/
select *
from test.user
where
-- #choose
    -- #when :id != blank
        id = :id
    -- #break
    -- #when :name != blank
        and name = :name
    -- #break
    -- #default
        and enabled = true
    -- #break
-- #end
;
```

---

## 8. 常见错误与排查建议

### 8.1 SQL 对象没有以 `;` 结尾

XQL 解析依赖 `;` 识别 SQL 对象边界。如果上一个 SQL 没有 `;`，下一个名称出现时会报错：

```text
The sql which before the name 'xxx' does not seem to end with the ';'
```

解决：检查每个 SQL 对象末尾是否有 `;`。

### 8.2 重复 SQL 名称

同一文件中不能重复定义名称：

```text
Duplicate name 'xxx'
```

解决：保持 SQL 名称唯一。

### 8.3 控制指令没有成对出现

例如写了 `#if` 但漏写 `#fi`，会抛出脚本语法错误：

```text
has script syntax error
```

解决：检查块级指令的开闭配对和嵌套顺序。

### 8.4 模板名称冲突

内联模板提取后会作为 `${name}` 注册。如果已经存在同名模板，会报：

```text
The template name 'xxx' in SQL 'yyy' has already been defined before.
```

解决：保持模板名唯一。

### 8.5 内联模板缺少结束标记

```text
Inline template missing '//TEMPLATE-END'
```

解决：确保每个 `//TEMPLATE-BEGIN:xxx` 都有对应 `//TEMPLATE-END`，且不能嵌套。

### 8.6 参数名与 `#var` 变量名冲突

内部变量不应与输入参数同名，否则可能在绑定阶段产生冲突。

解决：`#var` 使用区别于入参的名称，例如 `safeAge`、`finalId`。

### 8.7 数字比较使用字符串值

`>`、`>=`、`<`、`<=` 要求数值比较：

```sql
-- #if :age > 10
```

如果传入 `age` 是字符串 `"10"`，且比较值也是字符串，则按数字解析。若非数字字符串，比较会报错。

解决：保证参与大小比较的入参是数字类型。

### 8.8 在 SQL IDE 中出现不完整片段高亮

独立模板 `/*{where}*/` 如果只有 `where id = :id`，会被 IDE 视为不完整 SQL。

解决：

- 优先使用内联模板；
- 或者把连接符放在 SQL 主体中，模板只放稳定片段；
- 借助 rabbit-sql IDEA 插件预览模板合并效果。

### 8.9 使用插件测试动态 SQL

项目已包含 rabbit-sql IDEA 插件能力，可在开发阶段直接测试动态 SQL：

- 自动识别 SQL 中的参数；
- 没有数据源时可以查看动态 SQL 计算结果；
- 配置数据源后可以直接执行并查看结果。

开发建议：

- 复杂动态 SQL 先在插件中验证；
- 确认不同参数组合下生成的 SQL 都符合预期；
- 再提交到测试环境。

---

## 9. 与官方源码模块的对应关系

### 9.1 XQL 文件解析

核心类：

```text
com.github.chengyuxing.sql.XQLFileManager
```

关键方法：

- `parseXql(...)`：解析文件为 `Resource`；
- `scanSql(...)`：将 SQL 字符串编译为 `Sql` 对象；
- `parseMetadata(...)`：解析 `-- @key value`；
- `appendInlineTemplate(...)`：提取内联模板；
- `mergeSqlTemplate(...)`：合并独立模板和 constants；
- `get(name, args)`：执行动态 SQL 并返回最终 SQL 与内部变量。

### 9.2 动态 SQL 脚本引擎

核心类：

```text
com.github.chengyuxing.common.script.RabbitScriptEngine
com.github.chengyuxing.common.script.lexer.RabbitScriptLexer
com.github.chengyuxing.common.script.lexer.IdentifierLexer
com.github.chengyuxing.common.script.ast.impl.RabbitScriptParser
com.github.chengyuxing.common.script.ast.impl.RabbitScriptEvaluator
```

它们分别负责：

- 将脚本按行切分；
- 识别指令 token；
- 构建 AST；
- 求值并输出最终文本。

### 9.3 命名参数预编译

核心类：

```text
com.github.chengyuxing.sql.util.SqlGenerator
```

最终 SQL 中的 `:name` 会被转换为 `?`，并记录参数名与索引的映射。

### 9.4 执行入口

核心类：

```text
com.github.chengyuxing.sql.BakiDao
com.github.chengyuxing.sql.support.JdbcSupport
```

`BakiDao#prepareSql` 完成：

1. 识别 `&alias.sqlName`；
2. 调用 `XQLFileManager#get(...)`；
3. 合并动态 SQL 内部变量；
4. 执行 `${}` 模板替换；
5. 调用 `SqlGenerator#generatePreparedSql`。

---

## 10. 总结

掌握 rabbit-sql 动态 SQL 与 XQL 文件管理，最核心的是理解以下几个层次：

1. **XQL 文件是普通 SQL 文件**  
   通过注释扩展名称、描述、元数据和模板，不破坏 SQL 标准。

2. **动态 SQL 在行注释中写控制指令**  
   指令必须成对出现，表达式支持变量、常量、逻辑、管道和路径访问。

3. **`:name` 与 `${name}` 是两套机制**  
   `:name` 会预编译为 `?`，`${name}` 是字符串模板替换。

4. **`#for` 是构造预编译 `in` 和动态 `update` 的关键**  
   它会生成唯一内部命名参数，再由 `SqlGenerator` 统一编译。

5. **模板尽量用内联模板**  
   既能复用条件，又能保持每个 SQL 对象完整，避免 IDE 误报。

按照这些原则组织 `.xql` 文件，可以让 SQL 具备结构清晰、可复用、可测试、可安全绑定的工程化能力。
