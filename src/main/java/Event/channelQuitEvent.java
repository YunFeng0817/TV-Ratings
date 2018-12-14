package Event;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class channelQuitEvent extends channelEvent {
    private String channel;
    private String show;

    channelQuitEvent(int typeID, String recorder) {
        super(typeID);
        super.eventFormat = commonPrefix + "5" + baseForm + detailTime + detailTime + wildcard + wildcard + wildcard + caughtWildcard + caughtWildcard + wildcard + wildcard + wildcard + wildcard + "\\|\\d+" + recordTimeFormat; // messageID|5|随机序列|CA卡号|序列号|结束时间|开始时间|ServiceID|TSID|频点|频道名称|节目名称|授权|信号强度|信号质量|是否SDV节目|持续时间|时间
        Pattern eventFormatPattern = Pattern.compile(super.eventFormat);
        Matcher eventFormatMatcher = eventFormatPattern.matcher(recorder);
        if (eventFormatMatcher.find()) {
            super.channel = eventFormatMatcher.group(2);
            super.show = eventFormatMatcher.group(3);
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(recordDateTimeFormat);
            super.recordTime = LocalDate.parse(eventFormatMatcher.group(4).substring(0, 14), dateTimeFormatter);
        }
    }
}
