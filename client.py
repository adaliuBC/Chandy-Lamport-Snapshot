#!/usr/bin/python
# -*- coding: UTF-8 -*-
import socket
import threading
import pickle
import sys
import time
import copy
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
global initID2localState
global initID2channelMsgList, initID2ifRecordMsgChannel
global initID2completeChannelSenderList
initID2localState = {}
initID2channelMsgList = {}
initID2ifRecordMsgChannel = {}
initID2completeChannelSenderList = {}
for id in idList:
    initID2localState[id] = None           # local state (balance)
    initID2ifRecordMsgChannel[id] = {}     # channel是否存msg {initID => {senderID => T/F}}
    initID2channelMsgList[id] = {}         # channel存的msg list
    initID2completeChannelSenderList[id] = []  # 已经收到第二个MARKER、不再记录的channel list
global snapshotList
snapshotList = {}  # The snapshot list I received as init process
balance = 10.0

def id2ind(id):
    return ord(id) - ord('A')

def ind2id(ind):
    return chr(ord('A') + ind)

def listening():  # new thread for each incoming msg
    while True:
        conn, addr = connListen.accept()
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
        subListenThread = threading.Thread(
            target = messageProcessing, args = (conn, )
        )
        subListenThread.daemon = True
        subListenThread.start()

def messageProcessing(connListen):
    global balance, snapshotList, initID2localState
    data = connListen.recv(1024)
    while True:
        if not data:  
            connListen.close()
            break
        else:
            msg = decode(data)
            print(f"{prefixWhite}CLIENT {id}: Receive message: {msg}{postfix}")
            cmd = msg[0]
            if cmd == "TRANSFER":  # receive TRANSFER msg
                senderID = msg[1]
                amount = msg[2]
                assert senderID in idList, f"{prefixRed}ERROR: Wrong senderID {senderID} in TRANSFER msg!{postfix}"
                # update balance
                balance += int(amount)
                print(f"{prefixYellow}CLIENT {id}: Receive ${amount} from CLIENT {senderID}{postfix}")
                print(f"{prefixYellow}CLIENT {id}: Current balance: ${balance}{postfix}")
                # if is recording msg for this channel during SNAPSHOT, save msg in channel
                # print(f"{prefixGreen}CLIENT {id}: recordingChannel: {initID2ifRecordMsgChannel}{postfix}")
                for initID in idList:
                    if initID2ifRecordMsgChannel[initID] \
                       and len(initID2ifRecordMsgChannel[initID]) > 0 \
                       and initID2ifRecordMsgChannel[initID][senderID] == True:
                        initID2channelMsgList[initID][senderID].append(msg)
            
            elif cmd == "MARKER":  # receive MARKER msg
                initID = msg[1]
                senderID = msg[2]
                # judge if first MARKER
                if len(initID2ifRecordMsgChannel[initID]) == 0: # is first MARKER
                    # reset channel msg list
                    for posSenderID in connFromList:
                        initID2channelMsgList[initID][posSenderID] = []
                    # record localState
                    initID2localState[initID] = balance

                    # record msgs from incoming channels
                    # print(f"{prefixGreen}CLIENT {id}: Start recording incoming msgs {msg}{postfix}")
                    for posSenderID in connFromList:
                        if senderID == posSenderID:  
                            # first MARKER's in channel, set as empty
                            initID2ifRecordMsgChannel[initID][posSenderID] = False
                        else:
                            # other in channel, begin recording
                            initID2ifRecordMsgChannel[initID][posSenderID] = True
                
                    # send MARKER to all outgoing channels
                    time.sleep(3)   # !sleep
                    for receiverID in connToList:
                        receiverInd = id2ind(receiverID)
                        conn = socket.socket()
                        conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                        conn.bind((addr, portConn))
                        targetAddr = addrList[receiverInd]
                        targetPortListen = portListenList[receiverInd]
                        conn.connect((targetAddr, targetPortListen))
                        data = ["MARKER", initID, id]  # send TRANSFER msg
                        print(f"{prefixWhite}CLIENT {id}: Send MARKER to client {receiverID}{postfix}")
                        conn.send(encode(data))
                        conn.close()

                    
                else:  # following MARKER for channel
                    # stop recording msg for this channel
                    initID2ifRecordMsgChannel[initID][senderID] = False

                # check if all incoming channels has received MARKER 
                isSnapshotTerminate = True
                if not initID2ifRecordMsgChannel[initID]:
                    isSnapshotTerminate = False
                for senderID, flag in initID2ifRecordMsgChannel[initID].items():
                    if flag != False:
                        isSnapshotTerminate= False
                if isSnapshotTerminate:  # if all MARKER are received, terminate SNAPSHOT process
                    # build SNAPSHOT msg
                    data = ["SNAPSHOT", id, initID2localState[initID]]
                    for senderID, state in initID2channelMsgList[initID].items():
                        channelState = [senderID, id, state]  
                        # channel is from sender to me，state if the list of msgs
                        data.append(channelState)
                    # send SNAPSHOT msg to init proc
                    time.sleep(3)
                    conn = socket.socket()
                    conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    conn.bind((addr, portConn))
                    targetAddr = addrList[id2ind(initID)]
                    targetPortListen = portListenList[id2ind(initID)]
                    conn.connect((targetAddr, targetPortListen))
                    conn.send(encode(data))
                    print(f"{prefixWhite}CLIENT {id}: Send SNAPSHOT to client {initID}{postfix}")
                    print(f"{prefixWhite}CLIENT {id}: {data}{postfix}")
                    conn.close()
                    ## clean up localState, inMarkerList, inMsgList
                    initID2localState[initID] = None
                    initID2ifRecordMsgChannel[initID] = {}
                    initID2channelMsgList[initID] = {}
                    initID2completeChannelSenderList[initID] = []

            elif cmd == "SNAPSHOT":
                # I am the init proc, I receive SNAPSHOT from all processes
                # save snapshot
                senderID = msg[1]
                senderLocalState = msg[2]
                senderChannelState = msg[3:]
                snapshotList[senderID] = [senderLocalState, senderChannelState]
                # if all snapshots are received, concat and print
                receiveAllSnapshot = True
                for pID in idList:
                    if pID not in snapshotList.keys():
                        receiveAllSnapshot = False
                if receiveAllSnapshot:
                    print(f"{prefixWhite}CLIENT {id}: SNAPSHOT complete{postfix}")
                    #print(f"{prefixGreen}CLIENT {id}: Balance ${initID2localState[id]}{postfix}")
                    for senderID, senderSnapshot in snapshotList.items():
                        senderLocalState = senderSnapshot[0]
                        senderChannelState = senderSnapshot[1:][0]
                        print(f"{prefixGreen}  CLIENT {senderID}: Balance ${senderLocalState}{postfix}")
                        if len(senderChannelState) > 0:
                            print(f"{prefixGreen}  Incoming channels for CLIENT {senderID}:{postfix}")
                            for channelState in senderChannelState:
                                chaSenderID = channelState[0]
                                chaReceiverID = channelState[1]
                                chaMsg = channelState[2]
                                print(f"{prefixGreen}    CLIENT {chaSenderID} -> CLIENT {chaReceiverID}: {chaMsg}{postfix}")
                    # print and delete
                    snapshotList = {}
                    initID2ifRecordMsgChannel[id] = {}
                    initID2channelMsgList[id] = {}
                    initID2localState[id] = None
                    # end of SNAPSHOT process
            
            else:
                print(f"{prefixRed}CLIENT{id} -- ERROR: Receive invalid msg {msg}{postfix}")
            data = connListen.recv(1024)


def inputProcessing():  # new thread for each input command
    while True:
        inp = input("Please input command:\n")
        inp = inp.split()
        if len(inp) == 0:
            print(f"{prefixRed}CLIENT {id}: ERROR - EMPTY COMMAND!{postfix}")
        elif inp[0] == "BALANCE":
            subInputThread = threading.Thread(
                target = balanceProcessing, args = (inp, )
            )
            subInputThread.daemon = True
            subInputThread.start()
        elif inp[0] == "TRANSFER":  # cmd format: TRANSFER targetClientID amount
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
        f"{prefixRed}CLIENT {id}: ERROR - Invalid Balance Command!{postfix}"
    print(f"{prefixYellow}CLIENT {id}: My balance is ${balance}{postfix}")

def transferProcessing(inp):
    global balance
    cmd = inp[0]
    targetID = inp[1]
    amount = int(inp[2])
    assert cmd == "TRANSFER", f"{prefixRed}CLIENT {id}: Invalid ERROR - Transfer Command!{postfix}"
    if balance < amount:  # balance not enough, fail transfer
        print(f"{prefixRed}CLIENT {id}: INSUFFICIENT BALANCE! balance = ${balance} < amount = ${amount}{postfix}")
    elif targetID not in connToList:
        print(f"{prefixRed}CLIENT {id}: NOT CONNECTED! CLIENT {id} --X-> CLIENT {targetID}{postfix}")
    else:  # success transfer
        print(f"{prefixYellow}CLIENT {id}: Current Balance: ${balance}{postfix}")
        # update balance
        balance -= amount
        # send transfer msg to targetClient
        time.sleep(3)   # !sleep
        conn = socket.socket()
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        conn.bind((addr, portConn))
        targetAddr = addrList[id2ind(targetID)]
        targetPortListen = portListenList[id2ind(targetID)]
        conn.connect((targetAddr, targetPortListen))
        data = ["TRANSFER", id, amount]  # send TRANSFER msg
        print(f"{prefixYellow}CLIENT {id}: Transfer ${amount} to client {targetID}{postfix}")
        conn.send(encode(data))
        print(f"{prefixYellow}CLIENT {id}: Current Balance: ${balance}{postfix}")
        conn.close()


def snapshotProcessing(inp):
    ## record local state
    global initID2localState
    global id, ind
    initID2localState[id] = copy.deepcopy(balance)    
    ## reset当前记录的incoming channels
    if initID2channelMsgList[id]:
        print(f"{prefixRed}CLIENT {id}: ERROR: in channel should be empty{postfix}")
        initID2channelMsgList[id] = {}
    ## starts recording incoming msgs on all incoming channels
    initID2ifRecordMsgChannel[id] = {}
    for senderID in connFromList:
        initID2ifRecordMsgChannel[id][senderID] = True
        initID2channelMsgList[id][senderID] = []   
    ## send MARKER on all outgoing channels
    time.sleep(3)   # !sleep
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
        print(f"{prefixWhite}CLIENT {id}: SNAPSHOT initial{postfix}")
        conn.send(encode(data))
        print(f"{prefixWhite}CLIENT {id}: Send MARKER to client {receiverID}{postfix}")
        conn.close()


# build connection
## get client index from param
## Who am I? ID given in argv
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
connListen.bind((addr, portListen))  #listening port
connListen.listen(5)

# thread used to process input
inputProcThread = threading.Thread(target=inputProcessing)
inputProcThread.daemon = True
inputProcThread.start()

# thread used to listen
listenThread = threading.Thread(target=listening)
listenThread.daemon = True
listenThread.start()

# main: sleep in dead loop
while True:
    time.sleep(5)
connListen.close()
