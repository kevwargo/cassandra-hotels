package cassdemo.backend;

import java.util.*;

public class CmdLine {
    public static BackendSession backendSession;
    public static final int stressedHotelID = 48167278;


    public static void run() throws BackendException {
        Scanner scanner = new Scanner(System.in);

        boolean run = true;
        while(run) {
            String cmd = scanner.nextLine();
            switch (cmd) {
                case "book":
                    book(scanner);
                    break;
                case "unbook":
                    unbook(scanner);
                    break;
                case "stress":
					stressTest();
					break;
				case "addHotel":
					addHotel(scanner);
					break;
                default:
                    System.out.println("Command was not recognized.");
                    break;
            }
        }
    }

    private static void book(Scanner scanner) throws BackendException
    {
        List<Integer> hotels = backendSession.getAllHotels();
        System.out.printf("Available hotels: ");
        for (Integer id : hotels){
            System.out.println(id);
        }
        System.out.println("Type: hotelId, customerName, count");
        String[] command = scanner.nextLine().split(" ");
        boolean result = backendSession.bookRooms(Integer.parseInt(command[0]),
                command[1], Integer.parseInt(command[2]));
        System.out.printf("Booking %s\n", result ? "success" : "fail");
    }

    private static void unbook(Scanner scanner) throws BackendException {
        System.out.println("Type: hotelId, customerName");
        String[] command = scanner.nextLine().split(" ");
        backendSession.unBookRooms(Integer.parseInt(command[0]), command[1]);
    }

	private static void addHotel(Scanner scanner) throws BackendException
    {
        System.out.println("Type: hotelId, name, freeRooms");
        String[] command = scanner.nextLine().split(" ");
        backendSession.addHotel(Integer.parseInt(command[0]),
                command[1], Integer.parseInt(command[2]));
        System.out.printf("Hotel %s with %d free rooms added.", command[1], Integer.parseInt(command[2]));
    }
    
    public static void runCustomer() throws BackendException {
        String name = UUID.randomUUID().toString();
        int booked = 0;
        for(int i = 0; i < 3; i++) {
			int roomCount = new Random().nextInt(6) + 1;
            if (backendSession.bookRooms(stressedHotelID, name, roomCount)) {
				booked += roomCount;
                System.out.printf("Customer %s booked %d rooms.\n", name, roomCount);
            } else {
                System.out.printf("Customer %s failed to book %d rooms.\n", name, roomCount);
            }
        }
		List<Integer> occupiedRooms = backendSession.getOccupiedRooms(stressedHotelID, name);
        if (occupiedRooms.size() < booked) {
			System.out.printf("Customer %s booked %d, but occupied %d.\n", name, booked, occupiedRooms.size());
		}
		backendSession.unBookRooms(stressedHotelID, name);
    }

    private static void createStressedHotel() throws BackendException {
        backendSession.addHotel(stressedHotelID, "stressTest", 300);
    }


    public static void stressTest() {
        try {
            createStressedHotel();
            List<Thread> threads = new ArrayList<Thread>();
            for(int i = 0; i < 500; i++) {
                Thread t = new Thread() {
                    public void run() {
						try {
							runCustomer();
						catch (BackendException be) {
							be.printStackTrace();
						}
                    }
                };
                threads.add(t);
                t.start();
            }
            for (Thread t : threads) {
				try {
					t.join();
				} catch (InterruptedException ie) {}
			}
        } catch (BackendException e) {
            e.printStackTrace();
        } finally {
			backendSession.deleteAll();
		}

    }
}
