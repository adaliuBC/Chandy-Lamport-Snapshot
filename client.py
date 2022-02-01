#!/usr/bin/python
# -*- coding: UTF-8 -*-
import socket
import threading
import pickle
import sys
import time
from encoding import *

global connListen
global id, ind
global balance
balance = 10.0

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
    global balance
    print("PROCESSING MESSAGE")
    # TODO
    data = conn.recv(1024)
    while True:
        if not data:  
            conn.close()
            break
        else:
            msg = decode(data)
            print(f"\033[0;37;40mCLIENT {id}: Receive message: {msg}\033[0m")
            cmd = msg[0]
            if cmd == "TRANSFER":
            # receive TRANSFER
                ## add amount
                senderId = msg[1]
                amount = msg[2]
                assert senderID in ["A", "B", "C", "D"], f"Wrong senderID {senderID} in TRANSFER msg!"
                balance += int(amount)
            elif cmd == "MARKER":
            # receive MARKER
                print(f"CLIENT {id}: Receive a MARKER!")
                ## TODO here
            else:
                print(f"\033[1;32;40mCLIENT{id} -- ERROR: Receive invalid msg {msg}\033[0m")
            data = conn.recv()


def inputProcessing():
    while True:
        inp = input("Please input command:\n")
        inp = inp.split()
        if inp[0] == "BALANCE":
            subInputThread = threading.Thread(
                target = balanceProcessing, args = (inp, )
            )
            subInputThread.daemon = True
            subInputThread.start()
        elif inp[0] == "TRANSFER":  # TRANSFER targetClientID amount
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

def balanceProcessing(inp):
    cmd = inp[0]
    assert cmd == "BALANCE", f"Invalid Balance Command!"
    print(f"CLIENT {id}: My balance is ${balance}.")

def transferProcessing(inp):
    global balance
    print("TRANSFERING")
    cmd = inp[0]
    targetID = inp[1]
    amount = int(inp[2])
    assert cmd == "TRANSFER", f"Invalid Transfer Command!"
    if balance < amount:  # can not transfer
        print(f"CLIENT {id}: INSUFFICIENT BALANCE! balance = ${balance} < amount = ${amount}")
    else:  # success transfer
        print(f"CLIENT {id}: Transfering to client {targetID}")
        # TODO here
        # update balance
        balance -= amount
        # send transfer msg to targetClient
        conn = socket.socket()
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        conn.bind((addr, portConn))
        targetAddr = addrList[id2ind(targetID)]
        targetPortListen = portListenList[id2ind(targetID)]
        conn.connect((targetAddr, targetPortListen))
        data = ["TRANSFER", id, amount]  # send TRANSFER msg
        # time.sleep(3)
        conn.send(encode(data))
        conn.close()


def snapshotProcessing(inp):
    print("SNAPSHOTING")
    # TODO here

# build connection
## get client index from param
## Who am I?
print(sys.argv)
if len(sys.argv) != 2:
    print(f"\033[0;31;40mERROR: Wrong command line parameter number!\033[0m")
if sys.argv[1] not in ['A', 'B', 'C', 'D']:
    print(f"\033[0;31;40mERROR: Wrong command line parameters!\033[0m")
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
