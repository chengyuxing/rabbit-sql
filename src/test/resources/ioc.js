var ioc = {
    sqlFileManager: {
        type: 'com.github.chengyuxing.sql.SQLFileManager',
        fields: {
            constants: {
                db: "qbpt_deve"
            },
            sqlMap: {
                data: 'pgsql/data.sql',
                other: 'pgsql/other.sql',
                mac: 'file:/Users/chengyuxing/Downloads/local.sql'
            },
            sqlList: [
                'pgsql/data.sql',
                'pgsql/other.sql'
            ]
        }, events: {
            create: 'init'
        }
    }
}