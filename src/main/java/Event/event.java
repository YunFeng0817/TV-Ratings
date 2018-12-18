package Event;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Date;
import java.util.regex.*;

public class event implements Serializable {
    private int typeID;
    Date recordTime; // record generation time
    Date eventTIme; // the detailed time the event occurs
    String CACardID; // the identification of one user(CA card number)
    static final String commonPrefix = "^\\d+\\|"; // messageID|
    static final String baseForm = "\\|\\w{17}\\|(\\d{15,16})\\|\\d{17}"; // |随机序列|CA卡号|序列号
    static final String detailTime = "\\|\\d{4}\\.\\d{2}\\.\\d{2}\\s\\d{2}:\\d{2}:\\d{2}"; // |yyyy.mm.dd hh:mm:ss
    static final String baseFormPlus = baseForm + detailTime;
    static final String recordTimeFormat = "\\|(\\d{17})"; // e.g. |20160502035932193
    static final String wildcard = "\\|.*";
    static final String caughtWildcard = "\\|(.*)";
    static final String recordDateTimeFormat = "yyyyMMddkkmmssSSS"; // the parse format for DateFormat class
    static final String eventDateTimeFormat = "yyyy.MM.dd kk:mm:ss"; // the parse format for DateFormat class
    String eventFormat;

    public event() {
    }

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
            } catch (ParseException e) {
                return null;
            }
        }
        return null;
    }

    public int getTypeID() {
        return typeID;
    }

    public String getCACardID() {
        return CACardID;
    }

    public void setCACardID(String CACardID) {
        this.CACardID = CACardID;
    }

    public void setTypeID(int typeID) {
        this.typeID = typeID;
    }

    public Timestamp getRecordTime() {
        return new Timestamp(recordTime.getTime());
    }

    public void setRecordTime(Timestamp recordTime) {
        this.recordTime = new Date(recordTime.getTime());
    }

    @Override
    public String toString() {
        return "Event type: " + this.typeID + " recorder time: " + recordTime;
    }
}
