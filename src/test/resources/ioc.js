var ioc = {
    sqlFileManager: {
        type: 'rabbit.sql.dao.SQLFileManager',
        fields: {
            namedPaths: {
                data: 'pgsql/data.sql',
                other: 'pgsql/other.sql',
                mac: 'file:/Users/chengyuxing/Downloads/local.sql'
            },
            unnamedPaths: [
                'pgsql/data.sql',
                'pgsql/other.sql'
            ]
        }
    }
}