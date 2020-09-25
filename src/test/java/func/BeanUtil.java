package func;

import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BeanUtil {
    private static final Map<Class<?>, SerializedLambda> CLASS_LAMBDA = new ConcurrentHashMap<>();

    public static <T> String convert2fieldName(FieldFunc<T> func) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        SerializedLambda lambda = getLambda(func);
        String methodName = lambda.getImplMethodName();
        String prefix;
        if (methodName.startsWith("get")) {
            prefix = "get";
        } else if (methodName.startsWith("is")) {
            prefix = "is";
        } else {
            throw new NoSuchMethodException("无效的Getter方法:" + methodName);
        }
        return toLowerCaseFirstOne(methodName.replace(prefix, ""));
    }

    static String toLowerCaseFirstOne(String s) {
        if (Character.isLowerCase(s.charAt(0)))
            return s;
        else
            return Character.toLowerCase(s.charAt(0)) + s.substring(1);
    }

    static SerializedLambda getLambda(Serializable fn) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Class<?> lambdaClass = fn.getClass();
        if (!CLASS_LAMBDA.containsKey(lambdaClass)) {
            Method method = lambdaClass.getDeclaredMethod("writeReplace");
            method.setAccessible(true);
            SerializedLambda lambda = (SerializedLambda) method.invoke(fn);
            CLASS_LAMBDA.put(lambdaClass, lambda);
            return lambda;
        }
        return CLASS_LAMBDA.get(lambdaClass);
    }
}
