"""

table data manager for the candelabra project.

has a few issues -
1. in general there should be something i'm doing to have to prevent needing to make
	updates in several places in order for one change to be made. i.e. i would normally
	use a shared structure to make the fields struct, create table statements, and the
	relevant queries so that people can avoid the tedium of making sure their change is
	correct. i just didn't do that here to save myself some time.
2. i am not very familiar with sqlite. i haven't used it much before and so i am likely
	not making use of anything that makes what i am doing below much easier. i am generally
	familiar with rdbms dbs but this specific built-in library i have very little time with.
3. these tables do not do anything apart from expanding the api response with the request
	parameter for 'market' given to the api.


"""

import sqlite3 as sl
import os

CREATE_TRADE_EXECUTIONS_TABLE = """
CREATE TABLE IF NOT EXISTS TRADE_EXECUTIONS (
	id integer,
	market text,
	liquidation integer,
	price real,
	side text,
	size real,
	time real
);
"""

CREATE_AGG_HISTORICAL_TRADE_DATA_TABLE = """
CREATE TABLE IF NOT EXISTS AGG_HIST_TRADE (
	market text,
	startTime text,
	open real,
	close real,
	high real,
	low real,
	volume real,
	resolution real
);
"""

CREATE_OPEN_INTEREST_TABLE = """
CREATE TABLE IF NOT EXISTS OPEN_INT_INTERVAL (
	market text,
	volume real,
	nextFundingRate real,
	nextFundingTime text,
	expirationPrice real,
	predictedExpirationPrice real,
	openInterest real,
	strikePrice real
);
"""

# to be imported in main to help with making db tuples into dicts
t_fields = {
	'TRADE_EXECUTIONS': ['id', 'market', 'liquidation', 'price', 'side', 'size', 'time'],
	'AGG_HIST_TRADE': ['market', 'startTime', 'open', 'close', 'high', 'low', 'volume', 'resolution'],
	'OPEN_INT_INTERVAL': ['market', 'volume', 'nextFundingRate', 'nextFundingTime',
	                      'expirationPrice', 'predictedExpirationPrice', 'openInterest',
	                      'strikePrice']
}

class TableManager:

	def __init__(self):
		"""init class
			make db file
			set connection
		"""
		path = os.path.dirname(__file__)
		self.conn = sl.connect(os.path.join(path, 'tradeblock.db'))
		self.make_tables()

	def make_tables(self):
		"""makes tables
		"""
		cursor = self.conn.cursor()
		cursor.execute(CREATE_TRADE_EXECUTIONS_TABLE)
		cursor.execute(CREATE_AGG_HISTORICAL_TRADE_DATA_TABLE)
		cursor.execute(CREATE_OPEN_INTEREST_TABLE)
		cursor.close()

	def store(self, data, table):
		"""writes data into table

		:param data:
		:param table:
		"""
		cursor = self.conn.cursor()
		insert_statement = f'INSERT INTO {table}({",".join(t_fields[table])}) VALUES ({",".join(["?"] * len(t_fields[table]))})'
		for d in data:
			cursor.execute(insert_statement, tuple(d[x] for x in t_fields[table]))
		self.conn.commit()
		cursor.close()

	def get_trades(self, start):
		"""gets all trades > timestamp

		:param start: unix timestamp
		:return: trades
		"""
		cursor = self.conn.cursor()
		query_statement = 'select * from TRADE_EXECUTIONS where time >= ?'
		trades = list(cursor.execute(query_statement, (start,)))
		cursor.close()
		return trades

	def get_recent_candle(self, resolution):
		"""gets recent candle

		:param resolution: to control which candle we pull
		:return: candle
		"""
		cursor = self.conn.cursor()
		query_statement = 'select market,max(startTime),open,close,high,low,volume,resolution from AGG_HIST_TRADE where resolution = ? group by resolution'
		recent_candle = list(cursor.execute(query_statement, (resolution,)))
		cursor.close()
		return recent_candle

	def fix_candle(self, updated_candle):
		"""fix candle data by updating the table

		:param updated_candle: from main.py method
		"""
		cursor = self.conn.cursor()
		update_statement = f'update AGG_HIST_TRADE set high=?,low=?,volume=? where resolution=? and startTime=?'
		cursor.execute(update_statement, (updated_candle['high'], updated_candle['low'], updated_candle['volume'], updated_candle['resolution'], updated_candle['startTime']))
		self.conn.commit()
		cursor.close()

	def shutdown(self):  # should've made this something i could use in a context manager
						 # so that way i can have this call as part of the __exit__ but
						 # i did not take the time to do that
		"""closes sqlite connection
		"""
		self.conn.close()
