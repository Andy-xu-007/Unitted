import java.util.UUID;

public class 唯一随机数 {
    public static void main(String[] args) {
        String str = UUID.randomUUID().toString().replaceAll("-", "");

//        for (int i=0; i<10; i++) {
//            System.out.println(UUID.randomUUID().toString().replaceAll("-", ""));
//        }

        String str1 = "abcd";
        System.out.println(str1.length());
        System.out.println(str1.substring(0, str1.length()-1));
    }
}
