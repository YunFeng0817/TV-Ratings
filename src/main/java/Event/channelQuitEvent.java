package Event;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class channelQuitEvent extends channelEvent {
    private String channel;
    private String show;
    private long lastTime; // The length of time when the user watch the show

    public channelQuitEvent() {
    }

    channelQuitEvent(int typeID, String recorder) throws ParseException {
        super(typeID);
        super.eventFormat = commonPrefix + "5" + baseForm + detailTime + detailTime + wildcard + wildcard + wildcard + caughtWildcard + caughtWildcard + wildcard + wildcard + wildcard + wildcard + "\\|(\\d+)" + recordTimeFormat; // messageID|5|随机序列|CA卡号|序列号|结束时间|开始时间|ServiceID|TSID|频点|频道名称|节目名称|授权|信号强度|信号质量|是否SDV节目|持续时间|时间
        Pattern eventFormatPattern = Pattern.compile(super.eventFormat);
        Matcher eventFormatMatcher = eventFormatPattern.matcher(recorder);
        if (eventFormatMatcher.find()) {
            super.CACardID = eventFormatMatcher.group(1);
            super.channel = eventFormatMatcher.group(2);
            super.show = eventFormatMatcher.group(3);
            DateFormat dateFormat = new SimpleDateFormat(recordDateTimeFormat);
            this.lastTime = Long.parseLong(eventFormatMatcher.group(4));
            super.recordTime = dateFormat.parse(eventFormatMatcher.group(5));
        }
    }

    public long getLastTime() {
        return lastTime;
    }

    public void setLastTime(long lastTime) {
        this.lastTime = lastTime;
    }
}
