package Event;

import java.io.Serializable;
import java.text.ParseException;
import java.util.Date;
import java.util.regex.*;

public abstract class event implements Serializable {
    private final int typeID;
    Date recordTime; // record generation time
    Date eventTIme; // the detailed time the event occurs
    static final String commonPrefix = "^\\d+\\|"; // messageID|
    private static final String baseForm = "\\|\\w{17}\\|(\\d{15,16})\\|\\d{17}"; // |随机序列|CA卡号|序列号
    private static final String detailTime = "\\|\\d{4}\\.\\d{2}\\.\\d{2}\\s\\d{2}:\\d{2}:\\d{2}"; // |yyyy.mm.dd hh:mm:ss
    static final String baseFormPlus = baseForm + detailTime;
    static final String recordTimeFormat = "\\|(\\d{17}).*"; // e.g. |20160502035932193
    static final String wildcard = "\\|.*";
    static final String caughtWildcard = "\\|(.*)";
    static final String recordDateTimeFormat = "yyyyMMddkkmmssSSS"; // the parse format for DateFormat class
    static final String eventDateTimeFormat = "yyyy.MM.dd kk:mm:ss"; // the parse format for DateFormat class
    String eventFormat;

    event(int typeID, String recorder) {
        this.typeID = typeID;
    }

    public static event eventFactory(String recorder) {
        int typeID;
        String getTypeFormat = commonPrefix + "(\\d+)" + wildcard;
        Pattern getTypePattern = Pattern.compile(getTypeFormat);
        Matcher getTypeMatcher = getTypePattern.matcher(recorder);
        if (getTypeMatcher.find()) {
            typeID = Integer.parseInt(getTypeMatcher.group(1));
            try {
                switch (typeID) {
                    case 1:

                        return new openEvent(typeID, recorder);

                    case 2:
                        return new closeEvent(typeID, recorder);
                    case 5:
                        return null;
                    case 21:
                        return null;
                    default:
                        return null;
                }
            } catch (ParseException e) {
                return null;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "Event type: " + this.typeID + " recorder time: " + recordTime;
    }
}
