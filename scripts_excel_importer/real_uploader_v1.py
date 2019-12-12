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

	def connectoraws(self):
		self.mydb = mysql.connector.connect(
			host="database-1.crzmygc22edi.us-east-1.rds.amazonaws.com",
			user="admin",
			passwd="",
			database="mydatabase"
			)
		self.mycursor = self.mydb.cursor()
	
	def connectorwamp(self):
		self.mydb = mysql.connector.connect(
			host="localhost",
			user="root",
			passwd="",
			database="exo"
			)
		self.mycursor = self.mydb.cursor()
		self.table_name = "pim_brand_size"

	def hidden_remover(self):
		with open('codelist.txt', 'r') as f:
			self.identifiants = f.read().splitlines()
		with open('namelist.txt', 'r') as f:
			self.names = f.read().splitlines()
		self.files = [f for f in listdir('./files/') if isfile(join('./files/', f))]
		for self.file in self.files:
			self.match = re.search(r'\d{7}', self.file)
			df = pd.read_excel('./files/' + self.file, sheet_name='BAREME-MEASUREMENT CHART', header=1)
			indexx = 1
			suppresor = 0
			col_suppresor = 0
			while str(df.columns[0]).count('TAILLES') == 0:
				suppresor += 1
				indexx += 1
				df = pd.read_excel('./files/' + self.file, sheet_name='BAREME-MEASUREMENT CHART', header=indexx)

			while str(df.columns[-1]).count('Unnamed') > 0:
				df = df.drop(df.columns[-1], axis=1)

			if str(df.columns[2]).count('Unnamed') > 0:
				df = df.drop(df.columns[2], axis=1)
				col_suppresor += 1

			wb = xlrd.open_workbook('./files/' + self.file, formatting_info=1)
			ws = wb.sheet_by_index(0)
			self.hidden_rows[self.file] = []
			try:
				for rowx in range(ws.nrows):
					if ws.rowinfo_map[rowx].hidden > 0:
						self.hidden_rows[self.file].append(rowx-2-suppresor)
			except:
				print('index bug')

			for colx in range(ws.ncols):
				if ws.colinfo_map[colx].hidden > 0:
					self.hidden_cols[self.file].append(colx - col_suppresor)

			df = df.drop(df.index[self.hidden_rows[self.file]])
			df = df.drop(df.columns[self.hidden_cols[self.file]], axis=1)	
			df.rename({df.columns[1]: 'code'}, axis=1, inplace=True)
			if str(df.columns[2]).count('38/40') >0:
				df.rename({df.columns[2]: '38/40'}, axis=1, inplace=True)
			elif str(df.columns[2]).count('\n38') >0:
				df.rename({df.columns[2]: '38'}, axis=1, inplace=True)
			else:
				print('ALERT')
				self.failed_counter += 1
				self.failed_file.append(self.file)

			df = df.iloc[:, :-3]
			df['id'] = self.file
			df.drop
			df.set_index('id', drop=True, inplace=True)
			self.df = df[df['code'].astype(str).str.contains('|'.join(self.identifiants), na=False) ]
			while str(self.df.columns[-1]).count('Unnamed') > 0:
				self.df = self.df.drop(self.df.columns[-1], axis=1)
			sql_class.sql_uploader()
			print(self.file)

	def sql_uploader(self):
		for i in range(len(self.df.index)):
			for n in range(1, len(self.df.columns)):
				identi = "SELECT id FROM pim_product WHERE model_id IN (SELECT id FROM pim_model WHERE model_number=%s) AND brand_size_id IN (SELECT id FROM pim_brand_size WHERE name=%s)"
				args = (str(self.match.group()), str(self.df.columns[n]))
				self.mycursor.execute(identi, args)
				myresult = self.mycursor.fetchall()
				try:
					index_namelist = self.identifiants.index(str(self.df.iloc[i][self.df.columns[1]]))
				except Exception as e:
					continue
				if len(myresult) > 0:
					pass
				else:
					continue
				if str(self.df.iloc[i][self.df.columns[0]]).count('1/2') > 0:
					multiplier = 20
				else:
					multiplier = 10

				for result in range(len(myresult)):
					try:
						sql = "INSERT INTO pim_sizeguide_product (id, {0}, created_at, updated_at) VALUES (%s,%s, %s, %s)".format(str(self.names[index_namelist]))
						val = (
							myresult[result][0],
							str(self.df.iloc[i][self.df.columns[n]]*multiplier),
							'2019-08-12',
							'2019-08-12',
							)
						self.mycursor.execute(sql, val)
						self.mydb.commit()

					except mysql.connector.IntegrityError as e:
						sql = "UPDATE pim_sizeguide_product SET {0} = {1} WHERE id={2}".format(str(self.names[index_namelist]), str(self.df.iloc[i][self.df.columns[n]]*multiplier), myresult[result][0])
						self.mycursor.execute(sql)
						self.mydb.commit()

					except mysql.connector.DatabaseError as e:
						continue

					except ValueError as e:
						continue


	def ender(self):
		print('Script successfully ended')
		# print('number of records successulf : ' + str(self.success_counter))
		# print('number of records NOT successulf : ' + str(self.failed_counter))
		# for i in self.failed_number:
		# 	print('-------------------------')
		# 	print(repr(i))
		# 	print('-------------------------')
		# with open("failed_files.txt", "a", newline='\n') as fp:
		# 	for i in self.failed_file:
		# 		#fp.write('---------------------------' + '\n')
		# 		fp.write(i + '\n')

if __name__ == '__main__':
	sql_class = Sql()
	sql_class.connectorwamp()
	sql_class.hidden_remover()	
	sql_class.ender()