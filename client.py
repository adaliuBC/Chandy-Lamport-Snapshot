#!/usr/bin/python
# -*- coding: UTF-8 -*-
import socket
import threading
import pickle
import sys
import time
from encoding import *

prefixGreen  = "\033[1;32;40m"
prefixRed    = "\033[1;31;40m"
prefixWhite  = "\033[0;37;40m"
prefixYellow = "\033[1;33;40m"
postfix = "\033[0m"
idList = ["A", "B", "C", "D"]


global connListen
global id, ind
global balance
global localState
global initID2localState, initID2haschannelMarker
global initID2channelMsgList, initID2ifRecordMsgChannel
global initID2completeChannelSenderList
initID2localState = {}
initID2haschannelMarker = {}
initID2channelMsgList = {}
initID2ifRecordMsgChannel = {}
initID2completeChannelSenderList = {}
for id in idList:
    initID2localState[id] = None           # local state (balance)
    initID2ifRecordMsgChannel[id] = False  # channel是否存msg
    initID2haschannelMarker[id] = {}       # channel是否已收到首个MARKER
    initID2channelMsgList[id] = {}         # channel存的msg list
    initID2completeChannelSenderList[id] = []  # channel已经收到第二个MARKER的sender list
global snapshotList
snapshotList = {}  # 我作为init proc收到的snapshot列表
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

def messageProcessing(connListen):
    global balance, snapshotList
    print("PROCESSING MESSAGE")
    data = connListen.recv(1024)
    while True:
        if not data:  
            connListen.close()
            break
        else:
            msg = decode(data)
            print(f"{prefixWhite}CLIENT {id}: Receive message: {msg}{postfix}")
            cmd = msg[0]
            if cmd == "TRANSFER":
            # receive TRANSFER
                # 更新 balance
                senderID = msg[1]
                amount = msg[2]
                assert senderID in idList, f"{prefixRed}Wrong senderID {senderID} in TRANSFER msg!{postfix}"
                balance += int(amount)
                print(f"{prefixYellow}CLIENT {id}: Receive ${amount} from CLIENT {senderID}")
                print(f"{prefixYellow}CLIENT {id}: Current balance: ${balance}")
                # 如果正在snapshot，在对应channel中存msg
                for initID in idList:
                    if initID2ifRecordMsgChannel[initID] == True:
                        if not initID2channelMsgList[initID]:
                            initID2channelMsgList[initID][senderID] = [msg]
                        else:
                            initID2channelMsgList[initID][senderID].append(msg)
            
            elif cmd == "MARKER":
            # receive MARKER
                print(f"{prefixWhite}CLIENT {id}: Receive a MARKER!{postfix}")
                initID = msg[1]
                senderID = msg[2]
                # judge if first MARKER for this channel
                if senderID not in initID2haschannelMarker[initID]: # is first MARKER for channel
                    initID2haschannelMarker[initID][senderID] = True
                    # reset channel msg list
                    initID2channelMsgList[initID][senderID] = None
                    # record localState
                    initID2localState[initID] = balance
                    # send MARKER to all outgoing channels
                    # time.sleep(3)
                    for receiverID in connToList:
                        receiverInd = id2ind(receiverID)
                        conn = socket.socket()
                        conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                        conn.bind((addr, portConn))
                        targetAddr = addrList[receiverInd]
                        targetPortListen = portListenList[receiverInd]
                        print(f"{prefixRed}{targetAddr}:{targetPortListen}{postfix}")
                        conn.connect((targetAddr, targetPortListen))
                        data = ["MARKER", initID, id]  # send TRANSFER msg
                        print(f"{prefixGreen}CLIENT {id}: Send MARKER to client {receiverID}{postfix}")
                        conn.send(encode(data))
                        conn.close()
                    # record msgs from incoming channels
                    initID2ifRecordMsgChannel[initID] = True
                else:  # following MARKER for channel
                    # 停止记录channel的新msgs
                    initID2ifRecordMsgChannel[initID] = False
                    initID2completeChannelSenderList[initID].append(senderID)

                # 检查是否所有incoming channels都收到了MARKER 
                ## TODO: ？？？第几个MARKER？
                isSnapshotTerminate = True
                for senderID in connFromList:
                    senderInd = id2ind(senderID)
                    if senderID not in initID2completeChannelSenderList[initID]:
                        isSnapshotTerminate = False
                if isSnapshotTerminate:  # 如果都收到了，就terminate
                    # build SNAPSHOT msg
                    data = ["SNAPSHOT", id, initID2localState[initID]]
                    for senderID, state in initID2channelMsgList[initID].items():
                        channelState = [senderID, id, state]  
                        # sender->me的channel，state是msg list
                        data.append(channelState)
                    # send SNAPSHOT msg to init proc
                    conn = socket.socket()
                    conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    conn.bind((addr, portConn))
                    targetAddr = addrList[id2ind(initID)]
                    targetPortListen = portListenList[id2ind(initID)]
                    print(f"{prefixRed}{targetAddr}:{targetPortListen}{postfix}")
                    conn.connect((targetAddr, targetPortListen))
                    # time.sleep(3)
                    conn.send(encode(data))
                    print(f"{prefixYellow}CLIENT {id}: Send SNAPSHOT to client {initID}{postfix}")
                    conn.close()

                    ## clean up localState, inMarkerList, inMsgList
                    initID2localState[initID] = None
                    initID2ifRecordMsgChannel[initID] = False
                    initID2haschannelMarker[initID] = {}
                    initID2channelMsgList[initID] = {}
                    initID2completeChannelSenderList[initID] = []
            elif cmd == "SNAPSHOT":
                # I am the init proc, I receive SNAPSHOT from all processes
                # save snapshot
                senderID = msg[1]
                senderLocalState = msg[2]
                senderChannelState = msg[3:]
                snapshotList[senderID] = [senderLocalState, senderChannelState]
                # 如果收到所有snapshot，就concat并且print
                receiveAllSnapshot = True
                for pID in idList:
                    if pID not in snapshotList.keys():
                        receiveAllSnapshot = False
                if receiveAllSnapshot:
                    print(f"{prefixGreen}CLIENT {id}: SNAPSHOT complete{postfix}")
                    print(f"{prefixGreen}CLIENT {id}: Balance ${initID2localState[id]}{postfix}")
                    for senderID, senderSnapshot in snapshotList.items():
                        senderLocalState = senderSnapshot[0]
                        senderChannelState = senderSnapshot[1:]
                        print(f"{prefixGreen}  CLIENT {senderID}: Balance ${senderLocalState}{postfix}")
                        print(f"{prefixGreen}  Incoming channels for CLIENT {id}:")
                        for channelState in senderChannelState:
                            chaSenderID = channelState[0]
                            chaReceiverID = channelState[1]
                            chaMsg = channelState[2]
                            print(f"{prefixGreen}    CLIENT {chaSenderID} -> CLIENT {chaReceiverID}: {chaMsg}{postfix}")
                    # print and delete
                    snapshotList = {}
                    initID2ifRecordMsgChannel[id] = False
                    initID2channelMsgList[id] = None
                    initID2localState[id] = None
                    # 彻底结束snapshot流程
            
            else:
                print(f"{prefixRed}CLIENT{id} -- ERROR: Receive invalid msg {msg}{postfix}")
            data = connListen.recv(1024)


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
            print(f"{prefixRed}CLIENT {id}: ERROR - INVALID COMMAND!{postfix}")

def balanceProcessing(inp):
    cmd = inp[0]
    assert cmd == "BALANCE", \
        f"{prefixRed}CLIENT {id}: Invalid Balance Command!{postfix}"
    print(f"{prefixYellow}CLIENT {id}: My balance is ${balance}{postfix}")

def transferProcessing(inp):
    global balance
    #print("TRANSFERING")
    cmd = inp[0]
    targetID = inp[1]
    amount = int(inp[2])
    assert cmd == "TRANSFER", f"{prefixRed}CLIENT {id}: Invalid Transfer Command!{postfix}"
    if balance < amount:  # can not transfer
        print(f"{prefixRed}CLIENT {id}: INSUFFICIENT BALANCE! balance = ${balance} < amount = ${amount}{postfix}")
    else:  # success transfer
        print(f"{prefixYellow}CLIENT {id}: Current Balance: ${balance}{postfix}")
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
        print(f"{prefixYellow}CLIENT {id}: Transfer ${amount} to client {targetID}{postfix}")
        # time.sleep(3)
        conn.send(encode(data))
        print(f"{prefixYellow}CLIENT {id}: Current Balance: ${balance}{postfix}")
        conn.close()


def snapshotProcessing(inp):
    ## record local state
    global localState
    global id, ind
    initID2localState[id] = balance
    ## send MARKER on all outgoing channels
    # time.sleep(3)
    for receiverID in connToList:
        receiverInd = id2ind(receiverID)
        targetAddr = addrList[receiverInd]
        targetPortListen = portListenList[receiverInd]
        # build conn for this port
        conn = socket.socket()
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        conn.bind((addr, portConn))
        conn.connect((targetAddr, targetPortListen))
        data = ["MARKER", id, id]  # ["MARKER", initID, senderID]
        print(f"{prefixGreen}CLIENT {id}: SNAPSHOT initial{postfix}")
        conn.send(encode(data))
        print(f"{prefixGreen}CLIENT {id}: Send MARKER to client {receiverID}{postfix}")
        conn.close()
    
    ## reset当前记录的incoming channels
    if initID2channelMsgList[id]:
        print(f"{prefixRed}CLIENT {id}: ERROR: in channel should be empty{postfix}")
        initID2channelMsgList[id] = None
    ## starts recording incoming msgs on all incoming channels
    initID2ifRecordMsgChannel[id] = True
        

    

# build connection
## get client index from param
## Who am I?
print(sys.argv)
if len(sys.argv) != 2:
    print(f"{prefixRed}ERROR: Wrong command line parameter number!{postfix}")
if sys.argv[1] not in idList:
    print(f"{prefixRed}ERROR: Wrong command line parameters!{postfix}")
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
