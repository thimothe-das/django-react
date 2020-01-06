import mysql.connector
import csv
import os
from os import listdir
from os.path import isfile, join
from pandas import DataFrame, read_csv, ExcelWriter
import pandas as pd
import xlwt
import xlrd
from xlwt import Workbook 
import re
import time
from collections import defaultdict


class Sql:

	def __init__(self):
		self.success_counter = 0
		self.failed_counter = 0
		self.failed_file = []
		self.hidden_rows = defaultdict(list)
		self.hidden_cols = defaultdict(list)
		self.failed_number = []

	def connectorwamp(self):
		self.mydb = mysql.connector.connect(
			host="localhost",
			user="root",
			passwd="",
			database="need",
			port=3308
			)
		self.mycursor = self.mydb.cursor()
		self.table_name = "pim_brand_size"

		self.mycursor.execute("select * from migrations")
		myresult = self.mycursor.fetchall()
		for x in myresult:
		  print(x)



sql_class = Sql()
sql_class.connectorwamp()

