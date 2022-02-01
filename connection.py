#!/usr/bin/python
# -*- coding: UTF-8 -*-
import pickle

addr = ["127.0.0.1"] * 4
portListen = [1024, 1025, 1026, 1027]
portConn = [2024, 2025, 2026, 2027]
connection = [
    ("A", "B"),
    ("B", "A"), ("B", "D"),
    ("C", "B"), 
    ("D", "A"), ("D", "B"), ("D", "C")
]
with open("connection.pickle", "wb+") as f:
    pickle.dump(addr, f)
    pickle.dump(portListen, f)
    pickle.dump(portConn, f)
    pickle.dump(connection, f)
with open("connection.pickle", "rb") as f:
    addr = pickle.load(f)
    portListen = pickle.load(f)
    portConn = pickle.load(f)
    conn = pickle.load(f)
    print(addr)
    print(portListen)
    print(portConn)
    print(conn)