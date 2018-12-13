import java.io.Serializable;

public class User implements Serializable {
    final private String CACard; // a unique number of CA card

    User(String CACard) {
        this.CACard = CACard;
    }
}
