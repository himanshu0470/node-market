import socketio
from multiprocessing import Process, Queue
from multiprocessing import Manager, Process
from multiprocessing.managers import BaseManager
import multiprocessing




manager = Manager()

NOTCREATED=0
OPEN=1
INACTIVE=2
SUSPEND=3
CLOSE=4
SETTLED=5
CANCEL=6
WIN=7
LOSE=8

BALL=1
OVER=2

TEST_VAL=2

PREMATCH=1
INPLAY=2
PREMATCHINPLAY=3


#create Type
AUTO=1
PREDEFINED=2

#market type categories
OVERSESSION=10
ONLYOVER=11
PLAYER=12
WICKET=31

SESSION=23
FANCYLDO=26
ONLYOVERLDO=27
LASTDIGITNUMBER=28
PLAYERBOUNDARIES=29
PLAYERBALLSFACED=30
PARTNERSHIPBOUNDARIES=32
WICKETLOSTBALLS=33
ODDEVEN=35
TOTALEVENTRUN=36
MIDOVERSESSION= 39

sio = socketio.Client()


#Global Variables for local data
LD_OVERSESSION_MARKETS=manager.dict()
LD_SESSION_MARKETS=manager.dict()
LD_SESSIONLDO_MARKETS=manager.dict()
LD_LASTDIGIT_MARKETS=manager.dict()
LD_ONLYOVER_MARKETS=manager.dict()
LD_ONLYOVERLDO_MARKETS=manager.dict()
LD_MARKET_TEMPLATE=manager.dict()
LD_IPL_DATA=manager.dict()
LD_LDO_RUNNERS=manager.dict()
LD_ODDEVEN_MARKETS=manager.dict()
LD_TOTALEVENTRUN_MARKETS=manager.dict()

LD_PLAYER_MARKETS=manager.dict()
LD_PLAYERBOUNDARY_MARKETS=manager.dict()
LD_FOW_MARKETS=manager.dict()
LD_PLAYERBALLSFACED_MARKETS=manager.dict()
LD_PARTNERSHIPBOUNDARIES_MARKETS=manager.dict()
LD_WICKETLOSTBALLS_MARKETS=manager.dict()
LD_MIDOVERSESSION_MARKETS=manager.dict()

LD_DEFAULT_VALUES=manager.dict()
LD_OVER_BALLS=manager.dict()


DYNAMICLINE=1
STATICLINE=2


# @sio.on('connect')
# def on_connect():
#     print('Connected to server')
# token = 'c3f93b55-b5b4-4bdc-80fd-a9ef06d436d2'
# sio.connect('https://panelapi.deployed.live/',auth={'token': token} ,transports=['websocket'])

# #sio.connect('http://localhost:3000/',auth={'token': token} ,transports=['websocket'])


# def create_socket_connection(token, url='https://panelapi.deployed.live/'):
#     """
#     Create and return a new Socket.IO client instance.
#     """
#     sio = socketio.Client()

#     @sio.on('connect')
#     def on_connect():
#         print(f"[{id(sio)}] Connected to server")

#     try:
#         sio.connect(url, auth={'token': token}, transports=['websocket'])
#         print(f"[{id(sio)}] Connection established")
#         return sio
#     except Exception as e:
#         print(f"Failed to connect socket: {e}")
#         return None
# token = 'c3f93b55-b5b4-4bdc-80fd-a9ef06d436d2'
# url = 'https://panelapi.deployed.live/'

#sockets = [create_socket_connection(token, url) for _ in range(12)]




queue = Queue()

def socket_manager():
    @sio.event
    def connect():
        print('Connected to the server.')
    token = 'c3f93b55-b5b4-4bdc-80fd-a9ef06d436d2'
    # Connect to the socket server
    sio.connect('https://uatpanelapi.deployed.live/',auth={'token': token} ,transports=['websocket'])

    # Handle outgoing messages
    while True:
        data = queue.get()
        if data is None:  # Shutdown signal
            break
        try:
            sio.emit('updatedEventMarket', data)
            print("Data sent successfully")
        except Exception as e:
            print("Failed to send data:", e)

    # Disconnect safely
    sio.disconnect()

# Start the socket manager process
socket_process = Process(target=socket_manager)
socket_process.start()
def cleanup_socket():
    """Send a shutdown signal to the socket manager process and join it."""
    queue.put(None)
    socket_process.join()