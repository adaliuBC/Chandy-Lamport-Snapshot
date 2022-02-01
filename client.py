#!/usr/bin/python
# -*- coding: UTF-8 -*-
import socket
import threading
import pickle
import sys
import time

global connListen
global id, ind

def id2ind(id):
    return ord(id) - ord('A')

def ind2id(ind):
    return chr(ord('A') + ind)

def listening():
    while True:
        conn, addr = connListen.accept()
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
        subListenThread = threading.Thread(
            target = messageProcessing, args = (conn, )
        )
        subListenThread.daemon = True
        subListenThread.start()

def messageProcessing(conn):
    print("PROCESSING MESSAGE")
    # TODO
    # 


def inputProcessing():
    while True:
        inp = input("Please input command:\n")
        inp = inp.split()
        if inp[0] == "TRANSFER":
            subInputThread = threading.Thread(
                target = transferProcessing, args = (inp, )
            )
            subInputThread.daemon = True
            subInputThread.start()
        elif inp[0] == "SNAPSHOT":
            subInputThread = threading.Thread(
                target = snapshotProcessing, args = (inp, )
            )
            subInputThread.daemon = True
            subInputThread.start()
        else:
            print(f"\033[0;37;40mCLIENT {id}: ERROR - INVALID COMMAND!\033[0m")


def transferProcessing(inp):
    print("TRANSFERING")
    # TODO here


def snapshotProcessing(inp):
    print("SNAPSHOTING")
    # TODO here

# build connection
## get client index from param
## Who am I?
print(sys.argv)
if sys.argv[1] not in ['A', 'B', 'C', 'D']:
    raise ValueError(f"\033[1;32;40mClient name is {sys.argv[1]}, should be A, B, C or D!\033[0m")
id = sys.argv[1]
ind = id2ind(id)
## get (addr, portListen)
with open("connection.pickle", "rb") as f:
    addrList = pickle.load(f)
    portListenList = pickle.load(f)
    portConnList = pickle.load(f)
    connList = pickle.load(f)

# get my (addr, port)
addr = addrList[ind]
portListen = portListenList[ind]
portConn = portConnList[ind]
# get list of clients that are connected with me
connToList = []      # me -> other
connFromList = []    # other -> me
for connTuple in connList:
    if connTuple[0] == id:
        connToList.append(connTuple[1])
    elif connTuple[1] == id:
        connFromList.append(connTuple[0])
print(addr, portListen)
print("I am connect to: ", connToList)
print("I am connect by: ", connFromList)



# build listen socket
connListen = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM, proto=0)
connListen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
connListen.bind((addr, portListen))  #用于监听的port
connListen.listen(5)

inputProcThread = threading.Thread(target=inputProcessing)
inputProcThread.daemon = True
inputProcThread.start()

listenThread = threading.Thread(target=listening)
listenThread.daemon = True
listenThread.start()

# main: sleep in dead loop
while True:
    time.sleep(5)
connListen.close()
