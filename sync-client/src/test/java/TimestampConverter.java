import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class TimestampConverter {

    private static final ZoneId CHINA_ZONE = ZoneId.of("Asia/Shanghai");

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                    .withZone(CHINA_ZONE);

    public static String toChinaTime(long timestampMillis) {
        return FORMATTER.format(Instant.ofEpochMilli(timestampMillis));
    }

    public static void main(String[] args) {
        long timestamp = 1783073680000L;

        String chinaTime = toChinaTime(timestamp);

        System.out.println(chinaTime);
    }
}