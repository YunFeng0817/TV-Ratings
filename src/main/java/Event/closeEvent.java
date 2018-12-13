package Event;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class closeEvent extends event {

    closeEvent(int typeID, String recorder) throws ParseException {
        super(typeID);
        super.eventFormat = commonPrefix + "2" + baseFormPlus + recordTimeFormat; // messageID|2|随机序列|CA卡号|序列号|时间
        Pattern eventFormatPattern = Pattern.compile(super.eventFormat);
        Matcher eventFormatMatcher = eventFormatPattern.matcher(recorder);
        if (eventFormatMatcher.find()) {
            DateFormat dateFormat = new SimpleDateFormat(recordDateTimeFormat);
            super.recordTime = dateFormat.parse(eventFormatMatcher.group(2));
        }
    }
}
