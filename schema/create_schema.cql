CREATE KEYSPACE IF NOT EXISTS Hotels
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE Hotels;

CREATE TABLE FreeRooms (
	hotelId int,
	room int,
	PRIMARY KEY (hotelId, room)
);

CREATE TABLE OccupiedRooms (
	hotelId int,
	customer varchar,
	room int,
	PRIMARY KEY (hotelId, customer, room)
);

CREATE TABLE Hotels (
	id int,
	name varchar,
	PRIMARY KEY(id)
);

