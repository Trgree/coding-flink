/**
 * @author L
 * @date 2020/3/4
 */
public class Test {
    public static void main(String[] args) {
        ThreadLocal threadLocal = new ThreadLocal();
        threadLocal.set("");
        threadLocal.get();
        threadLocal.remove();
        int s =-3;
        System.out.println(Integer.toBinaryString(s));
    }
}
