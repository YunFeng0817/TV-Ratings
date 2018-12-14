package Event;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class closeEvent extends event {

    closeEvent(int typeID, String recorder){
        super(typeID);
        super.eventFormat = commonPrefix + "2" + baseFormPlus + recordTimeFormat; // messageID|2|随机序列|CA卡号|序列号|时间
        Pattern eventFormatPattern = Pattern.compile(super.eventFormat);
        Matcher eventFormatMatcher = eventFormatPattern.matcher(recorder);
        if (eventFormatMatcher.find()) {
            super.CACardID = eventFormatMatcher.group(1);
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(recordDateTimeFormat);
            super.recordTime = LocalDate.parse(eventFormatMatcher.group(2).substring(0, 14), dateTimeFormatter);
        }
    }
}
