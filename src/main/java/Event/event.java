package Event;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.regex.*;
import java.time.format.DateTimeParseException;

public abstract class event implements Serializable {
    private int typeID;
    LocalDate recordTime; // record generation time
    LocalDate eventTIme; // the detailed time the event occurs
    static final String commonPrefix = "^\\d+\\|"; // messageID|
    static final String baseForm = "\\|\\w{17}\\|(\\d{15,16})\\|\\d{17}"; // |随机序列|CA卡号|序列号
    static final String detailTime = "\\|\\d{4}\\.\\d{2}\\.\\d{2}\\s\\d{2}:\\d{2}:\\d{2}"; // |yyyy.mm.dd hh:mm:ss
    static final String baseFormPlus = baseForm + detailTime;
    static final String recordTimeFormat = "\\|(\\d{17})"; // e.g. |20160502035932193
    static final String wildcard = "\\|.*";
    static final String caughtWildcard = "\\|(.*)";
    static final String recordDateTimeFormat = "yyyyMMddHHmmss"; // the parse format for DateFormat class
    static final String eventDateTimeFormat = "yyyy.MM.dd kk:mm:ss"; // the parse format for DateFormat class
    String eventFormat;

    event(int typeID) {
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
//                        return null;
                    case 2:
                        return new closeEvent(typeID, recorder);
//                        return null;
                    case 5:
                        return new channelQuitEvent(typeID, recorder);
//                        return null;
                    case 21:
                        return new channelEnterEvent(typeID, recorder);
//                        return null;
                    case 23:
                        return new channelCollectEvent(typeID, recorder);
//                        return null;
                    case 97:
                        return new timeShiftShowEvent(typeID, recorder);
//                        return null;
                    default:
                        return null;
                }
            } catch (DateTimeParseException ignored) {
            }
        }
        return null;
    }

    public int getTypeID() {
        return this.typeID;
    }

    public void setTypeID(int typeID) {
        this.typeID = typeID;
    }

    public LocalDate getRecordTime() {
        return this.recordTime;
    }

    public void setRecordTime(LocalDate recordTime) {
        this.recordTime = recordTime;
    }

    @Override
    public String toString() {
        return "Event type: " + this.typeID + " recorder time: " + recordTime;
    }
}
