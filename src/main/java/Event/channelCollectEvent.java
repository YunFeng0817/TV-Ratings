package Event;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class is an abstract of the event that the user collect one liked show
 */
class channelCollectEvent extends channelEvent {
    channelCollectEvent(int typeID, String recorder) {
        super(typeID);
        super.eventFormat = commonPrefix + "23" + baseFormPlus + wildcard + wildcard + wildcard + caughtWildcard + caughtWildcard + wildcard + wildcard + wildcard + recordTimeFormat; // messageID|23|随机序列|CA卡号|序列号|时间|ServiceID|TSID|频点|频道名称|节目名称|授权|信号强度|信号质量|时间
        Pattern eventFormatPattern = Pattern.compile(super.eventFormat);
        Matcher eventFormatMatcher = eventFormatPattern.matcher(recorder);
        if (eventFormatMatcher.find()) {
            super.CACardID = eventFormatMatcher.group(1);
            super.channel = eventFormatMatcher.group(2);
            super.show = eventFormatMatcher.group(3);
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(recordDateTimeFormat);
            super.recordTime = LocalDate.parse(eventFormatMatcher.group(4).substring(0, 14), dateTimeFormatter);
        }
    }
}
