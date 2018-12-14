package Event;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.regex.*;

class openEvent extends event {
    openEvent(int typeID, String recorder) {
        super(typeID);
        super.eventFormat = commonPrefix + "1" + baseFormPlus + wildcard + wildcard + recordTimeFormat; // messageID|1|随机序列|CA卡号|序列号|时间|硬件版本号|软件版本号|时间
        Pattern eventFormatPattern = Pattern.compile(super.eventFormat);
        Matcher eventFormatMatcher = eventFormatPattern.matcher(recorder);
        if (eventFormatMatcher.find()) {
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(recordDateTimeFormat);
            super.recordTime = LocalDate.parse(eventFormatMatcher.group(2).substring(0, 14), dateTimeFormatter);
        }
    }
}
