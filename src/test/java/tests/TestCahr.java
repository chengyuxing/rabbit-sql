package tests;

import java.util.ArrayList;
import java.util.List;

public class TestCahr {
    public static void main(String[] args) {
        String s = "(((520+480)*38/10)/2*((520+480)*38/10)/2)) ";
        List<Char> list = new ArrayList<Char>();
        do {
            s = getString(s, list);
        } while (s != null && s.indexOf("(") != -1);
        for (Char c : list) {
            System.out.println(c.str.substring(c.startIndex, c.endIndex + 1));
        }
    }

    public static String getString(String str, List<Char> list) {
        char[] cs = str.toCharArray();
        boolean isStart = false;
        Char ch = new Char();
        ch.str = str;
        for (int i = 0; i < cs.length; i++) {
            char c = cs[i];
            if (c == '(') {
                if (!isStart) {
                    ch.startIndex = i;
                    isStart = true;
                }
                ch.layer++;
            } else if (c == ')' && ch.layer > 0) {
                ch.layer--;
                if (ch.layer == 0) {
                    ch.endIndex = i;
                    list.add(ch);
                    if (i != cs.length - 1) {
                        String last = str.substring(i + 1);
                        do {
                            last = getString(last, list);
                        } while (last != null && last.indexOf("(") != -1);
                    }
                    break;
                }
            }
        }
        if (ch.endIndex != 0) {
            return str.substring(ch.startIndex + 1, ch.endIndex);
        }
        return null;
    }

}

class Char {
    int startIndex;
    int endIndex;
    int layer;
    String str;
}
