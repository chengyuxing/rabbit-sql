var ioc = {
    sqlFileManager: {
        type: 'com.github.chengyuxing.sql.SQLFileManager',
        fields: {
            constants: {
                db: "qbpt_deve"
            },
            sqlMap: {
                sys: 'pgsql/test.sql',
                mac: 'file:/Users/chengyuxing/Downloads/local.sql'
            },
            sqlList: ['pgsql/test.sql']
        }, events: {
            create: 'init'
        }
    }
}