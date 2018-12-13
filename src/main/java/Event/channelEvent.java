package Event;

abstract class channelEvent extends event {
    String channel = "";
    String show = "";

    channelEvent(int typeID) {
        super(typeID);
    }

    @Override
    public String toString() {
        return super.toString() + (this.channel.isEmpty() ? "" : (" channel: " + this.channel)) + (this.show.isEmpty() ? "" : " show: " + this.show);
    }
}
