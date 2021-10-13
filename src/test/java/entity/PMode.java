package entity;

import com.github.chengyuxing.sql.support.IOutParam;

public enum PMode {
    IN {
        @Override
        public PValue put(Object value) {
            return new PValue(value, this, null);
        }
    },
    OUT {
        @Override
        public PValue put(IOutParam type) {
            return new PValue(null, this, type);
        }
    },
    IN_OUT {
        @Override
        public PValue put(Object value, IOutParam type) {
            return new PValue(value, this, type);
        }
    };

    public PValue put(Object value) {
        throw new AbstractMethodError();
    }

    public PValue put(IOutParam type) {
        throw new AbstractMethodError();
    }

    public PValue put(Object value, IOutParam type) {
        throw new AbstractMethodError();
    }

}
