import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Copyright (c) 2020-2030 All right Reserved
 *
 * @author :   Andy Xu
 * @version :   1.0
 * @date :   8/24/2020 5:04 PM
 */
public class propertiesParse {
    public static void main(String[] args) throws IOException {
        String p = "E:\\workspace\\Unitted\\Unitted\\src\\main\\resources\\test.properties";
        Properties prop = new Properties();
        prop.load(new java.io.FileInputStream(p));

        System.out.println("s1:   " + prop.getProperty("charing"));
        System.out.println(prop.getProperty("type").length());
        System.out.println("s3:   " + prop.getProperty("insertDT"));
        System.out.println("s4:   " + prop.getProperty("count"));

    }
}
