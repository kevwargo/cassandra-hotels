package cassdemo.backend;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Hotel {
    //private int hotelId;
    private List<Integer> freeRooms = new ArrayList<>();
    private Map<Integer, String> occupiedRooms = new HashMap<>();

    public List<Integer> getFreeRooms() {
        return freeRooms;
    }

    public Map<Integer, String> getOccupiedRooms() {
        return occupiedRooms;
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Free rooms:\n");
        for (Integer room : freeRooms) {
            builder.append(room);
            builder.append(" ");
        }
        builder.append("\nOccupied rooms:\n");
        for (Map.Entry<Integer, String> room : occupiedRooms.entrySet()) {
            builder.append(room.getKey());
            builder.append(" ");
            builder.append(room.getValue());
            builder.append("\n");
        }
        return builder.toString();
    }


}
