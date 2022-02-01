#!/usr/bin/python
# -*- coding: UTF-8 -*-
import json

def encode(string, encoding="utf-8"):
    return (json.dumps(string)).encode(encoding)

def decode(bytes, encoding="utf-8"):
    return json.loads(bytes.decode(encoding))