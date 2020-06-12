package distjob;

import java.util.HashSet;
import java.util.Set;

/**
 * @author wangjie
 * @date 2020/6/12 9:59
 */
public class EventBus {

    private  Set<EventType> events = new HashSet<>();

     public synchronized void   setEvent(EventType type){

        events.add(type);

    }

    public synchronized  Set<EventType> aqiureEvent() throws  InterruptedException{

         while (events.size() == 0){
             this.wait();
         }
         Set<EventType> ac = events;
         events = new HashSet<>();
         return ac;
    }




    public  enum EventType{
        EventType_ReShard(1),
        EventType_ReFreash(2);


        private EventType(int type){
            eventType = type;
        }


        int eventType;
    }
}



