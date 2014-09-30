package monitor;

import java.util.Date;

/**
 * Created by hp on 14-9-23.
 */
public class ParserMonitor {

    public Date startTimeDate;

    public Date endTimeDate;

    public int inEventNum;

    public int outEventNum;

    public int inSizeEvents;

    public int inSizeBytes;

    public int outSizeEvents;

    public long startDealTime;

    public long endDealTime;

    public long duringDealTime;

    public ParserMonitor() {
        startTimeDate = null;
        endTimeDate = null;
        inEventNum = outEventNum = inSizeEvents = outSizeEvents = inSizeBytes =  0;
        duringDealTime = startDealTime = endDealTime = 0L;
    }

    public void clear() {
        startTimeDate = null;
        endTimeDate = null;
        inEventNum = outEventNum = inSizeEvents = outSizeEvents =  0;
        duringDealTime = startDealTime = endDealTime = 0L;
    }

}
