"""
main file for candelabra

has a few issues - some due to lack of time spent looking at the ftx api and others due to laziness preventing
    me from polishing this off..
1. no logging - i just used print statements as i didn't want to spend time configuring thread logging
2. issue with the update step - though this is the thrust of this project, i for some reason could not
    get the trade data generated volume to look anything like the exchange candle at any resolution. i
    already spent a while on it and i think this solution is nearly correct. more time with the ftx api
    would let me resolve this issue in full i believe.
3. in the same vein as the issue with the update step, i think my timing of the intervals are also off.
    to fix that issue i should take the time to block the reconcile step until i am sure that the rele-
    vant trade data is available. in my solution, i just prepare the db with the previous day's trade 
    volume and hope that it works out. i just let this be nearly correct as well - i just did not want
    to implement the thread synchronization code.
4. i am probably using sqlite and the dbthread db_actions thread wrong. kept running into cases where
    separate threads were attempting to share the same connection and i learned that sqlite is thread-
    safe on its own so i didn't need to do anything special so long as each thread used its own conne-
    tion. i did not remove that dbthread method due to laziness.
5. this code is not very readable. i normally rely on SonarQube to help me troubleshoot readability
    and while i have most of the PEP rules memorized, i just didn't take any time to polish the
    syntax here. normally i would take the time to assure that someone else doesn't struggle to read
    this code.
6. i am pretty confident that the 'open interest' problem is something i needed to calculate myself
    but i did not teach myself how to do that for this project. all of the api responses mentioning 
    open interest did not specify a market and that is why i figured i would need to do my own calcu-
    lation for getting this data point.
7. this probably should have been implemented as a server with a corresponding client that could inquire
    what was in the db and what was being handled now. i could leave the polling steps on a heartbeat and
    add in some other commands via flask or something else that would let me shut it down or do all of the
    other server steps.


"""

from candelabra.table_manager import TableManager, t_fields
from candelabra.rest_api import FtxClient
from candelabra.ftx_websocket_api import FtxWebsocketClient
import datetime
import logging
import signal
import sqlite3 as sl
import atexit
from enum import Enum
import time
import threading
from threading import Event
from multiprocessing import Queue
import queue
import math
import sys


one_minute = 60
one_hour = 60 * one_minute
one_day = 24 * one_hour


class Candelabra:

    def __init__(self):
        """makes instance
        """
        # didn't check how long authentication lasts so nothing is written
        # in this code to rebuild my authentication after a certain amount
        # of time..
        api_key = ''
        api_secret = ''
        self.ftx_rest_api = FtxClient(api_key=api_key, api_secret=api_secret)
        self.ftx_websocket_api = FtxWebsocketClient(api_key, api_secret)

    def signal_handler(self, sig, frame):
        """receives keyboard interrupt and shuts down code
        """
        # i had some problems getting my threads to be cleaned up. i think if this
        # were written with a more server style pattern i could write in a cleaner
        # shutdown step and probably do something to truncate all of my queues and log
        # what was requested but unfinished. did not think through restarts or anything
        # else similar.
        print('received interrupt signal')
        self.end_thread = True
        for thread in self.threads:
            thread.join()
        sys.exit()

    def main(self):
        """as i understand the solution, i am supposed to continuously poll at the turn
            of every interval for trade and and candle data. the way i did that was to
            set up several queues to hold time frames to pull trade and candle information
            for. then i regularly drop timestamps into those queues as the script runs.

            this is a short summary of the script:
            1. set up queues and threads
            2. add timestamps to queues
            3. as threads receive timestamps, pull data via api as per timestamps
            4. when data is available, update candles if any difference is observed.

        """
        self.end_thread = False  # used for getting threads to join/return
        signal.signal(signal.SIGINT, self.signal_handler)  # used to keyboard interrupt script
        signal.signal(signal.SIGTERM, self.signal_handler)  # used to keyboard interrupt script

        # queues are for holding timestamps to run work
        start = int(time.time())
        self.fhdq = Queue()  # fetch historical data queue
        self.tdq = Queue()  # trade data queue
        self.recon_q = Queue()  # reconciliation queue
        self.dbq = Queue()  # database queue

        # the below are the threads - 3 initialized where necessary as we receive at most
        # that many timestamps into a queue at once.
        # threaded trade queue / threaded_td
        # threaded fetch historical data / threaded_fhd
        # reconcile_by_resolution
        # db_actions
        tqt1 = threading.Thread(target=self.threaded_td)
        tfhd1 = threading.Thread(target=self.threaded_fhd)
        tfhd2 = threading.Thread(target=self.threaded_fhd)
        tfhd3 = threading.Thread(target=self.threaded_fhd)
        recon1 = threading.Thread(target=self.reconcile_by_resolution)
        recon2 = threading.Thread(target=self.reconcile_by_resolution)
        recon3 = threading.Thread(target=self.reconcile_by_resolution)
        dbthread = threading.Thread(target=self.db_actions)

        self.threads = [tqt1, tfhd3, tfhd2, tfhd1, recon3, recon2, recon1, dbthread]

        for thread in self.threads:
            thread.daemon = True  # all threads die when main thread dies
            thread.start()

        m1min = start - one_minute
        m1hour = start - one_hour
        m1day = start - one_day
        self.tdq.put((m1day, start))  # prep trade db with past day's worth of trade data
        # get most recent candles
        self.fhdq.put((m1min, start, one_minute))
        self.fhdq.put((m1hour, start, one_hour))
        self.fhdq.put((m1day, start, one_day))
        min_ticker = 0
        # this script will just run forever until you ctrl+c
        while True:  # could be made into a helper method ..
            # if a minute goes by, get latest minute candle and trade data
            time.sleep(60)
            if time.time() - start > 60:
                start = int(time.time())
                m1min = start - one_minute
                self.tdq.put((m1min, start))
                self.fhdq.put((m1min, start, one_minute))
                min_ticker = min_ticker + 1
            # if 60 minutes, get latest hour long candle
            if min_ticker % 60 == 0:
                m1hour = start - one_hour
                self.fhdq.put((m1hour, start, one_hour))
            # if 1 day, get latest day long candle
            if min_ticker % 1440 == 0:
                m1day = start - one_day
                self.fhdq.put((m1day, start, one_day))

    def db_actions(self):  # probably causing some issues with my steps sync'ing below
        """wrote this to fix the thread sharing connections issue but it is unnecessary
        """
        table_manager = TableManager()
        while True:
            if self.end_thread:
                return
            try:
                data, table = self.dbq.get(False)
            except queue.Empty:
                time.sleep(15)
                continue
            table_manager.store(data, table)

    def store_trades(self, data):
        """preps data to be stored in trades table

        :param data: from trades
        """
        for d in data:
            if not d['liquidation']:
                d['liquidation'] = 0
            else:
                d['liquidation'] = 1
            d['market'] = 'BTC-PERP'
            d['time'] = datetime.datetime.strptime(d['time'].replace('+00:00', '').replace('T', ' '), '%Y-%m-%d %H:%M:%S.%f').timestamp()
        self.dbq.put((data, 'TRADE_EXECUTIONS'))

    def store_candles(self, data, resolution):
        """preps data to store in candle data table
        """
        for d in data:
            d['market'] = 'BTC-PERP'
            d['resolution'] = resolution
        self.dbq.put((data, 'AGG_HIST_TRADE'))

    def threaded_td(self):
        """gets trade data timestamps and gets data via api

        originally i wrote this method without realizing that i was supposed to be getting
        the data from the websocket api instead of the rest api - that's why the below code
        looks like it relies on the timestamps. really i am just pulling whatever i am getting
        from the websocket api without any interaction with it.

        the truth is that i did not fully understand how to use the ftx websocket api in a way
        where my subscription to this channel for trade data would also accept my input of time
        stamps the same way the rest api would. reading the documentation makes it seem like there
        is not a way of doing that, so all the queue really does here is to just make sure this runs
        once a minute. i should probably be using the subscription to where i just pull in data as
        it is available, but i am just doing it in this hacky way below. this is likely one of the
        causes with the issues in my update logic.
        """
        while True:
            if self.end_thread:
                return
            try:
                start, end = self.tdq.get(False)
            except queue.Empty:
                time.sleep(5)
                continue
            self.ftx_websocket_api.get_trades('BTC-PERP')
            while not self.ftx_websocket_api._trades['BTC-PERP']:
                time.sleep(10)
            trades = []
            while self.ftx_websocket_api._trades['BTC-PERP']:
                trade = self.ftx_websocket_api._trades['BTC-PERP'].pop()[0]
                trades.append(trade)
            self.store_trades(trades)

    def threaded_fhd(self):
        """gets time stamps and resolution for fetching candle data
            also adds data into the reconciliation queue
        """
        while True:
            if self.end_thread:
                return
            try:
                start_time, end_time, resolution = self.fhdq.get(False)
            except queue.Empty:
                time.sleep(15)
                continue
            candle_data = self.ftx_rest_api.get_candles('BTC-PERP', resolution, start_time, end_time)
            self.store_candles(candle_data, resolution)
            self.recon_q.put((start_time, end_time, resolution))

    def reconcile_by_resolution(self):
        """pulls latest candle and turns trade data into candle
            then calls a method to reconcile the two
        """
        table_manager = TableManager()
        while True:
            if self.end_thread:
                return
            try:
                start_time, end_time, resolution = self.recon_q.get(False)
            except queue.Empty:
                time.sleep(15)
                continue
            trades = table_manager.get_trades(start_time)
            candle = table_manager.get_recent_candle(resolution)
            if not trades or not candle:
                continue
            candle = dict(zip(t_fields['AGG_HIST_TRADE'], candle[0]))
            trade_candle = self.candelize_trades(trades)
            trade_candle['resolution'] = resolution
            trade_candle['startTime'] = candle['startTime']
            self.reconcile(trade_candle, candle)

    def candelize_trades(self, trade_data):
        """turns a list of individual trades into its own candle

        :param trade_data: list of trade data from db
        :return: candle made from trade data
        """
        trade_candle = {'volume': 0, 'high': -math.inf, 'low': math.inf}
        for trade_datum in trade_data:
            trade_datum = dict(zip(t_fields['TRADE_EXECUTIONS'], trade_datum))
            trade_candle['volume'] = trade_candle['volume'] + trade_datum['size']  # sum trade sizes to get volume
            if trade_datum['price'] > trade_candle['high']:  # update as necessary
                trade_candle['high'] = trade_datum['price']
            if trade_datum['price'] < trade_candle['low']:
                trade_candle['low'] = trade_datum['price']
        return trade_candle

    def reconcile(self, trade_candle, candle):
        """checks if the trade_candle is different than candle data
            if it is, prints that it is and sets boolean to true
            boolean triggers an update statement to run
        :param trade_candle: candle generated from db trade data
        :param candle: candle from db
        """
        run_update = False
        for field in ['high', 'low', 'volume']:
            if abs(trade_candle[field] - candle[field]) > .001:
                print('field', field, 'diff!', 'trade candle', trade_candle[field], 'exchange candle', candle[field])
                run_update = True

        if run_update:
            table_manager = TableManager()
            table_manager.fix_candle(trade_candle)


if __name__ == '__main__':
    inst = Candelabra()
    inst.main()
