var ioc = {
    sqlFileManager: {
        type: 'rabbit.sql.dao.SQLFileManager',
        fields: {
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