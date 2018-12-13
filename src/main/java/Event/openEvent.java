package Event;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.*;

class openEvent extends event {
    openEvent(int typeID, String recorder) throws ParseException {
        super(typeID, recorder);
        super.eventFormat = commonPrefix + "1" + baseFormPlus + wildcard + wildcard + recordTimeFormat; // messageID|1|随机序列|CA卡号|序列号|时间|硬件版本号|软件版本号|时间
        Pattern eventFormatPattern = Pattern.compile(super.eventFormat);
        Matcher eventFormatMatcher = eventFormatPattern.matcher(recorder);
        if (eventFormatMatcher.find()) {
            DateFormat dateFormat = new SimpleDateFormat("yyyyMMddkkmmssSSS");
            super.recordTime = dateFormat.parse(eventFormatMatcher.group(2));
        }
    }
}
