package Event;

abstract class channelEvent extends event {
    String channel = "";
    String show = "";

    channelEvent(int typeID, String recorder) {
        super(typeID, recorder);
    }

    @Override
    public String toString() {
        return super.toString() + (this.channel.isEmpty() ? "" : (" channel: " + this.channel)) + (this.show.isEmpty() ? "" : " show: " + this.show);
    }
}
