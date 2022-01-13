var ioc = {
    sqlFileManager: {
        type: 'com.github.chengyuxing.sql.XQLFileManager',
        fields: {
            constants: {
                db: "qbpt_deve"
            },
            files: {
                sys: 'pgsql/test.sql',
                mac: 'file:/Users/chengyuxing/Downloads/local.sql'
            }
        }, events: {
            create: 'init'
        }
    }
}