import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class time {
    public static void main(String[] args) {
        // 时间戳 Epoch Time，计算从1970年1月1日零点（格林威治时区／GMT+00:00）到现在所经历的秒数
        System.out.println(System.currentTimeMillis());

        // 过时
//        Date date = new Date();
//        System.out.println(date.getYear() + 1900);  // 必须加上1900，源码，过时
//        System.out.println(date.getMonth() + 1);  // 0~11，必须加上1
//        System.out.println(date.getDate());  // 1~31不能加1
//
//        System.out.println(date.toString());
//        System.out.println(date.toGMTString());  // 转为GMT时区
//        System.out.println(date.toLocaleString()); // 转为本地时区
//
//        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        SimpleDateFormat dateFormat1 = new SimpleDateFormat("E MMM dd, yyyy");
//        System.out.println(dateFormat.format(date));

        // LocalDateTime java8， ISO 8601规定的日期和时间分隔符是T
        // now返回的总是已当前默认时区返回
        LocalDate d = LocalDate.now();  // 当前日期
        LocalTime t = LocalTime.now();  // 当前时间
        LocalDateTime dt = LocalDateTime.now();  // 当前日期和时间
        LocalDate d_same = dt.toLocalDate();   //  日期截取
        LocalTime t_same = dt.toLocalTime();   //  时间截取
        System.out.println(d + "   " + t + "   " + dt);  // 严格按照ISO 8601格式打印

        LocalDate d2 = LocalDate.of(2020, 8, 31);  // 2020-8-31
        LocalTime t2 = LocalTime.of(15, 16, 17);  // // 15:16:17
        LocalDateTime dt2 = LocalDateTime.of(2019, 11, 30, 15, 16, 17);
        LocalDateTime dt3 = LocalDateTime.of(d2, t2);

        LocalDateTime dt4 = LocalDateTime.parse("2019-11-19T15:16:17");  // 字符串转为LocalDateTime
        LocalDate d4 = LocalDate.parse("2019-11-19");
        int dayOfMonth = d4.getDayOfMonth();  // 19
        LocalDate localDate = d4.minusDays(dayOfMonth);
        LocalDate localDate1 = localDate.minusMonths(6);
        System.out.println(localDate1 + "!!!!!!!!!");

        LocalTime t4 = LocalTime.parse("15:16:17");

        // 自定义格式化
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        System.out.println(dtf.format(LocalDateTime.now()));
        // 用自定义格式解析
        LocalDateTime dt5 = LocalDateTime.parse("2019/11/30 15:16:17", dtf);
        System.out.println(dt5);

        LocalDateTime dt6 = LocalDateTime.of(2019, 10, 26, 20, 30, 59);
        // 加5天减3小时:
        LocalDateTime dt7 = dt.plusDays(5).minusHours(3);
        // 减1月: 注意到月份加减会自动调整日期，例如从2019-10-31减去1个月得到的结果是2019-09-30，因为9月没有31日。
        LocalDateTime dt8 = dt2.minusMonths(1);
        // 对日期和时间调整：日期变为31日:
        LocalDateTime dt9 = dt.withDayOfMonth(31);
        // 对日期和时间调整：月份变为9: 同样注意到调整月份时，会相应地调整日期，即把2019-10-31的月份调整为9时，日期也自动变为30
        LocalDateTime dt10 = dt2.withMonth(9);



    }
}

