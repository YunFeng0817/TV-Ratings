package Event;

public abstract class channelEvent extends event {
    String channel = "";
    String show = "";

    channelEvent(int typeID) {
        super(typeID);
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getShow() {
        return show;
    }

    public void setShow(String show) {
        this.show = show;
    }

    @Override
    public String toString() {
        return super.toString() + (this.channel.isEmpty() ? "" : (" channel: " + this.channel)) + (this.show.isEmpty() ? "" : " show: " + this.show);
    }
}
