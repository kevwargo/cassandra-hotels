package cassdemo.backend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/*
 * For error handling done right see: 
 * https://www.datastax.com/dev/blog/cassandra-error-handling-done-right
 * 
 * Performing stress tests often results in numerous WriteTimeoutExceptions, 
 * ReadTimeoutExceptions (thrown by Cassandra replicas) and 
 * OpetationTimedOutExceptions (thrown by the client). Remember to retry
 * failed operations until success (it can be done through the RetryPolicy mechanism:
 * https://stackoverflow.com/questions/30329956/cassandra-datastax-driver-retry-policy )
 */

public class BackendSession {

	private static final Logger logger = LoggerFactory.getLogger(BackendSession.class);

	public static BackendSession instance = null;

	private Session session;

	public BackendSession(String contactPoint, String keyspace) throws BackendException {

		Cluster cluster = Cluster.builder()
            .addContactPoint(contactPoint)
            .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.QUORUM))
            .build();
		try {
			session = cluster.connect(keyspace);
		} catch (Exception e) {
			throw new BackendException("Could not connect to the cluster. " + e.getMessage() + ".", e);
		}
		prepareStatements();
	}

	private static PreparedStatement SELECT_FROM_FREE_ROOMS_BY_HOTEL;
	private static PreparedStatement INSERT_INTO_FREE_ROOMS;
	private static PreparedStatement DELETE_FROM_FREE_ROOMS;

	private static PreparedStatement INSERT_INTO_OCCUPIED_ROOMS;
	private static PreparedStatement SELECT_FROM_OCCUPIED_ROOMS_BY_HOTEL;
	private static PreparedStatement SELECT_FROM_OCCUPIED_ROOMS_BY_CUSTOMER;
	private static PreparedStatement DELETE_FROM_OCCUPIED_ROOMS;

    private static PreparedStatement SELECT_FROM_HOTELS;
    private static PreparedStatement INSERT_INTO_HOTELS;

	private static PreparedStatement DELETE_ALL_FREE_ROOMS;
	private static PreparedStatement DELETE_ALL_HOTELS;
	private static PreparedStatement DELETE_ALL_OCCUPIED_ROOMS;

	private void prepareStatements() throws BackendException {
		try {
			SELECT_FROM_FREE_ROOMS_BY_HOTEL = session.prepare(
					"SELECT * FROM FreeRooms WHERE hotelId = ?;");
			INSERT_INTO_FREE_ROOMS = session.prepare(
					"INSERT INTO FreeRooms (hotelId, room) VALUES (?, ?);");
			DELETE_FROM_FREE_ROOMS = session.prepare(
					"DELETE FROM FreeRooms WHERE hotelId = ? AND room = ?;");

			SELECT_FROM_OCCUPIED_ROOMS_BY_HOTEL = session.prepare(
					"SELECT * FROM OccupiedRooms WHERE hotelId = ?;");
			SELECT_FROM_OCCUPIED_ROOMS_BY_CUSTOMER = session.prepare(
					"SELECT * FROM OccupiedRooms WHERE hotelId = ? AND customer = ?;");
			INSERT_INTO_OCCUPIED_ROOMS = session.prepare(
					"INSERT INTO OccupiedRooms (hotelId, room, customer) VALUES (?, ?, ?);");
			DELETE_FROM_OCCUPIED_ROOMS = session.prepare(
					"DELETE FROM OccupiedRooms WHERE hotelId = ? AND room = ?;");

            SELECT_FROM_HOTELS = session.prepare(
                "SELECT * FROM Hotels;");
			INSERT_INTO_HOTELS = session.prepare(
				"INSERT INTO Hotels (id, name) VALUES (?, ?)");
			DELETE_ALL_FREE_ROOMS = session.prepare(
				"DELETE FROM FreeRooms;");
			DELETE_ALL_HOTELS = session.prepare(
				"DELETE FROM Hotels;");
			DELETE_ALL_OCCUPIED_ROOMS = session.prepare(
				"DELETE FROM OccupiedRooms;");
			
		} catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}

		logger.info("Statements prepared");
	}

	public Hotel getHotel(int hotelId) throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_FROM_FREE_ROOMS_BY_HOTEL);
		bs.bind(hotelId);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		Hotel hotel = new Hotel();
		for (Row row : rs) {
			int rRoom = row.getInt("room");
			hotel.getFreeRooms().add(rRoom);
		}

		bs = new BoundStatement(SELECT_FROM_OCCUPIED_ROOMS_BY_HOTEL);
		bs.bind(hotelId);

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		for (Row row : rs) {
			int rRoom = row.getInt("room");
			String rCustomer = row.getString("customer");
			hotel.getOccupiedRooms().put(rRoom, rCustomer);
		}

		return hotel;
	}

	public boolean bookRooms(int hotelId, String customer, int count) throws BackendException
	{
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_FROM_FREE_ROOMS_BY_HOTEL);
		bs.bind(hotelId);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		Hotel hotel = new Hotel();
		for (Row row : rs) {
			int rRoom = row.getInt("room");
			hotel.getFreeRooms().add(rRoom);
		}

		if(hotel.getFreeRooms().size() < count)
			return false;

		int processed = 0;
		for (Integer rRoom : hotel.getFreeRooms()) {
			if(processed >= count)
				break;

			bs = new BoundStatement(DELETE_FROM_FREE_ROOMS);
			bs.bind(hotelId, rRoom);
			try {
				session.execute(bs);
			} catch (Exception e) {
				throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
			}

			bs = new BoundStatement(INSERT_INTO_OCCUPIED_ROOMS);
			bs.bind(hotelId, rRoom, customer);
			try {
				session.execute(bs);
			} catch (Exception e) {
				throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
			}

			processed++;
		}


		return true;
	}

	public boolean unBookRooms(int hotelId, String customer) throws BackendException
	{
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_FROM_OCCUPIED_ROOMS_BY_CUSTOMER);
		bs.bind(hotelId, customer);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		List<Integer> rooms = new ArrayList<Integer>();
		for (Row row : rs) {
			rooms.add(row.getInt("room"));
		}

		for (Integer rRoom : rooms) {
			bs = new BoundStatement(DELETE_FROM_OCCUPIED_ROOMS);
			bs.bind(hotelId, rRoom);
			try {
				session.execute(bs);
			} catch (Exception e) {
				throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
			}

			bs = new BoundStatement(INSERT_INTO_FREE_ROOMS);
			bs.bind(hotelId, rRoom);
			try {
				session.execute(bs);
			} catch (Exception e) {
				throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
			}

		}
		return true;
	}
	
	public void addHotel(int hotelId, String name, int freeRooms) throws BackendException {
		BoundStatement bs = new BoundStatement(INSERT_INTO_HOTELS);
		bs.bind(hotelId, name);
		try {
            session.execute(bs);
        } catch(Exception e) {
            throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
        }
        for (int i = 0; i < freeRooms; i++) {
			bs = new BoundStatement(INSERT_INTO_FREE_ROOMS);
			bs.bind(hotelId, i + 1);
			try {
				session.execute(bs);
			} catch(Exception e) {
				throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
			}
		}
	}
	
	public List<Integer> getOccupiedRooms(int hotelId, String user) {
		BoundStatement bs = new BoundStatement(SELECT_FROM_OCCUPIED_ROOMS_BY_CUSTOMER);
		bs.bind(hotelId, user);
		ResultSet rs = null;
        List<Integer> result = new ArrayList<Integer>();
        try {
            rs = session.execute(bs);
        } catch(Exception e) {
            throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
        }
        for (Row row : rs){
            result.add(row.getInt("room"));
        }
        return result;
	}

	public List<Integer> getAllHotels() throws BackendException {
        BoundStatement bs = new BoundStatement(SELECT_FROM_HOTELS);
        ResultSet rs = null;
        List<Integer> result = new ArrayList<Integer>();
        try {
            rs = session.execute(bs);
        } catch(Exception e) {
            throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
        }
        for (Row row : rs){
            result.add(row.getInt("Id"));
        }
        return result;
    }
    
    public void deleteAll() {
		BoundStatement bs = new BoundStatement(DELETE_ALL_FREE_ROOMS);
		try {
            rs = session.execute(bs);
        } catch(Exception e) {
            e.printStackTrace();
        }
		bs = new BoundStatement(DELETE_ALL_HOTELS);
		try {
            rs = session.execute(bs);
        } catch(Exception e) {
            e.printStackTrace();
        }
		bs = new BoundStatement(DELETE_ALL_OCCUPIED_ROOMS);
		try {
            rs = session.execute(bs);
        } catch(Exception e) {
            e.printStackTrace();
        }
	}

	protected void finalize() {
		try {
			if (session != null) {
				session.getCluster().close();
			}
		} catch (Exception e) {
			logger.error("Could not close existing cluster", e);
		}
	}

}
