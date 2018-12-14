package Event;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class timeShiftShowEvent extends channelEvent {
    timeShiftShowEvent(int typeID, String recorder) {
        super(typeID);
        // messageID|97|随机序列|CA卡号|序列号|时间|ServiceID|TSID|频点|频道名称|节目名称|授权|信号强度|信号质量|当前时间点|执行动作|响应时间
        super.eventFormat = commonPrefix + "97" + baseFormPlus + wildcard + wildcard + wildcard + caughtWildcard + caughtWildcard + "(\\|.*){7}" + recordTimeFormat;
        Pattern eventFormatPattern = Pattern.compile(super.eventFormat);
        Matcher eventFormatMatcher = eventFormatPattern.matcher(recorder);
        if (eventFormatMatcher.find()) {
            super.channel = eventFormatMatcher.group(2);
            super.show = eventFormatMatcher.group(3);
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(recordDateTimeFormat);
            super.recordTime = LocalDate.parse(eventFormatMatcher.group(5).substring(0, 14), dateTimeFormatter);
        }
    }
}
