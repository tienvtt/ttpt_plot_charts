from datetime import datetime, timedelta
from glob import glob
import win32api
import win32con
import random
import time
import math
import csv
import os
import re
import json
import copy
import requests
import pyautogui
import shutil
import asyncio

import xlwings as xw
import numpy as np
import pandas as pd
from collections import defaultdict
from urllib.parse import quote_plus as qp
from sqlalchemy import create_engine, text

# # WEB DEPENDENCIES
# from selenium import webdriver
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.common.by import By
# from selenium.webdriver.common.keys import Keys
# from selenium.webdriver.common.action_chains import ActionChains
# from selenium.webdriver.firefox.options import Options
# from selenium.common.exceptions import TimeoutException

# MULTIPROCESSING DEPENDENCIES
import threading
import multiprocessing as mp
import concurrent.futures as concurrent
from multiprocessing import Pool, Manager, Queue

import PyPDF2
from PyPDF2 import PdfReader, PdfWriter, PdfMerger

# from googleapiclient.discovery import build
# from google.oauth2.service_account import Credentials

# import discord

from PIL import Image
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
