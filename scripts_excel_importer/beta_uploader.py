import mysql.connector
import csv
import os
from os import listdir
from os.path import isfile, join
import pandas as pd
import re
from collections import defaultdict
import numpy as np
import xlrd
import xlwt
from shutil import copyfile
import time
from time import gmtime, strftime
import logging

class Sql:

	def __init__(self):
		self.success_counter = 0
		self.failed_counter = 0
		self.failed_file = []
		self.hidden_rows = defaultdict(list)
		self.hidden_cols = defaultdict(list)
		self.failed_number = []
		self.last_file = ""
		self.start_time = time.time()
		self.counter_file = 0


	def logger(self):
		logging.basicConfig(format='%(asctime)s %(message)s', filename='logs.log',level=logging.DEBUG)
		# logging.debug('This message should appear on the console')
		# logging.info('So should this')
		# logging.warning('And this, too')

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
			database="test4",
			port=3308
			)
		self.mycursor = self.mydb.cursor()
		self.table_name = "pim_brand_size"
		logging.info('Successfully connected to DB')

	def getSizesBrand(self):
		sql = "SELECT name FROM pim_brand_size"
		self.mycursor.execute(sql)
		myresult = self.mycursor.fetchall()
		sizes = [item[0] for item in myresult]
		return sizes

	def wholeDfPrinter(self):
		with pd.option_context('display.max_rows', None, 'display.max_columns', None): 
		    print(self.df)

	def ProgressBar(self):
		files_number = len(self.files)
		self.counter_file += 1
		print("{} / {}".format(self.counter_file, files_number))
		

	def databaseMaxRows(self):
		self.files = [f for f in listdir('./files/') if isfile(join('./files/', f))]
		sql = "SELECT name FROM pim_brand_size"
		self.mycursor.execute(sql)
		size_list = self.mycursor.fetchall()
		line_counter = 0
		for self.file in self.files:
			match = re.search(r'\d{7}', self.file)
			for size in size_list:
				try:
					sql = "SELECT id FROM pim_product WHERE model_id IN (SELECT id FROM pim_model WHERE model_number=%s) AND brand_size_id IN (SELECT id FROM pim_brand_size WHERE name=%s)"
					args = (str(match[0]), str(size[0]))
				except:
					# No match was found so it shouldnt be uploaded to DB so we pass to the next loop
					continue
				self.mycursor.execute(sql, args)
				myresult = self.mycursor.fetchall()
				line_counter += len(myresult)
		print("The DB should have " + str(line_counter) + ' rows in it')
		logging.info("The DB should have {} rows in it".format(line_counter))


	def remove_hidden_rows(self):
		self.files = [f for f in listdir('./files/') if isfile(join('./files/', f))]
		for self.file in self.files:
			try:
				wb = xlrd.open_workbook('./files/' + self.file, formatting_info=1)
			except Exception as e:
				print('xlxs file, should be xls file')
				file_error = 'FILE : ' + str(self.file) + ' ERROR CODE : ' + str(e) + ' ERREUR : ' + 'Wrong file extension, should be xls (not xlxs)'
				self.failed_file.append(file_error)
				with open("failed_files" + ".csv", "a", newline='\n') as fp:
					a = csv.writer(fp)
					row = [str(self.file)] + ['Wrong file extension, should be xls (not xlxs)']
					a.writerow(row)
				copyfile("files/" + self.file, "failed_files/xlxsFiles/" + self.file)
				logging.info("Wrong file extension, should be xls (not xlxs), name of the file : {}".format(self.file))


			# wb_sheet = wb.sheet_by_index(0)

			# newBook = xlwt.Workbook()
			# newSheet = newBook.add_sheet("data")

			# idx = 0
			# try:
			# 	for row_idx in range(0, wb_sheet.nrows):
			# 		hidden = wb_sheet.rowinfo_map[row_idx].hidden
			# 		if (hidden == 0):
			# 			for col_index, cell_obj in enumerate(wb_sheet.row(row_idx)):
			# 				newSheet.write(idx, col_index, cell_obj.value)
			# 			idx = idx + 1
				
			# 	newBook.save('./files/noHiddenRows/' + self.file)
			# 	print("file without hiddens rows and columns saved successfully")
			# except Exception as e:
			# 	print(e)
			# 	copyfile("files/" + self.file, "failed_files/wentWrong/" + self.file)

			# 	print("Something went wrong")


			ws = wb.sheet_by_index(0)
			self.hidden_rows[self.file] = []
			try:
				for rowx in range(ws.nrows):
					if ws.rowinfo_map[rowx].hidden > 0:
						self.hidden_rows[self.file].append(rowx-2)
			except:
				print('index out of range')

			for colx in range(ws.ncols):
				if ws.colinfo_map[colx].hidden > 0:
					self.hidden_cols[self.file].append(colx)
				


	def clean_data(self):
		self.mycursor.execute("TRUNCATE TABLE pim_sizeguide_product")
		with open("failed_files" + ".csv", "w", newline='\n') as fp:
			a = csv.writer(fp)
			row = ['FILE NAME'] + ['ERROR EXPLAINED']
			a.writerow(row)
		dfCodes = pd.read_csv("codeName.csv")
		self.files = [f for f in listdir('./files/') if isfile(join('./files/', f))]
		for self.file in self.files:
			self.columnsMens = []
			print(self.file)

			self.match = re.search(r'\d{7}', self.file)
			sizesBrand = self.getSizesBrand()
			df = pd.read_excel('./files/' + self.file, header=1)
			try:
				df = df.drop(df.index[self.hidden_rows[self.file]])
				df = df.drop(df.columns[self.hidden_cols[self.file]], axis=1)
			except Exception as e:
				print('removal of hidden rows and columns wasnt successfull')
				print(e)
				logging.exception("Error : {}, removal of hidden rows and columns wasnt successfull : {}".format(e, self.file))

			df.columns.values[0] = "description"
			df.columns.values[1] = "code"
			columnsNames = ["description", "code"]
			sizes = []
			for columnName in df.columns:
				columnNameCleaned = str(columnName).replace('\n', "").replace("BASIC", "").replace("SIZE", "").strip()

				df = df.rename(columns={columnName: columnNameCleaned})
				if columnNameCleaned in sizesBrand:
					sizes.append(columnNameCleaned)
					columnsNames.append(columnNameCleaned)

			df = df[columnsNames]
			df = df[df['code'].astype(str).str.contains('|'.join(list(dfCodes["code"])), na=False)]
			try:
				for size in sizes:
					df[size] = df[size] * np.where(df['description'].astype(str).str.contains('1/2', na=False), 20, 10)
				df = df.drop(columns=["description"])
				df = df.set_index("code").T
				df = df.groupby(level=0, axis=1).max()
				for columnName in df.columns:
					str(columnName).strip()
					if columnName in self.getAllCodeNames():
						df = df.rename(columns={columnName: dfCodes.loc[dfCodes['code'] == columnName]["name"].values[0]})
					else:
						df = df.drop([columnName], axis=1)
				if len(df.columns) == len(set(df.columns)):
					copyfile("files/" + self.file, "failed_files/duplicatesRow/" + self.file)

				df = df.reset_index()
				df = df.rename(columns={"index": "size"})
				df = df.dropna(axis=1, how='all')
				df = df.dropna(axis=0, thresh=2)
				for columnName in df.columns:
					if columnName not in ["size", "model_number", "code"]:
						self.columnsMens.append(columnName)
				try:
					df["model_number"] = self.match.group()
				except Exception as e:
					file_error = 'FILE : ' + str(self.file) + ' ERREUR : ' + 'Pas d\'identifiant dans le nom du fichier'
					self.failed_file.append(file_error)
					with open("failed_files" + ".csv", "a", newline='\n') as fp:
						a = csv.writer(fp)
						row = [str(self.file)] + ['Pas d\'identifiant dans le nom du fichier']
						a.writerow(row)
						copyfile("files/"+self.file, "failed_files/noID/"+self.file)
					logging.error("Error : {}, Pas d\'identifiant dans le nom du fichier : {}".format(e, self.file))
				else:
					self.df = df
					sql_class.sql_uploader()
			except Exception as e:
				print(e)
				copyfile("files/" + self.file, "failed_files/wentWrong/" + self.file)
				print("Something went wrong")
				logging.error("Error : {}, something went wrong in the file : {}".format(e, self.file))
			sql_class.ProgressBar()




	def getAllCodeNames(self):
		dfCodeNames = pd.read_csv("codeName.csv")
		allCodenames = dfCodeNames["code"].tolist()
		return allCodenames


	def productidChecker(self, val):
		identi = "SELECT * FROM pim_sizeguide_product WHERE product_id={}".format(val[0])
		self.mycursor.execute(identi)
		myresult = self.mycursor.fetchall()
		if len(myresult) > 0:
			if self.last_file != self.file:
				with open("failed_files" + ".csv", "a", newline='\n') as fp:
					a = csv.writer(fp)
					row = [str(self.file)] + ["product id exist already in the DB"] + ['the file has the same 7 digits number than an other file already parsed'] 
					a.writerow(row)
				self.last_file = self.file
			logging.info("File : {}, the product id with the value {} exist already in the DB ".format(val[0], self.file))
			return True
		else:
			return False

	def sql_uploader(self):
		for index, row in self.df.iterrows():
			sql = "SELECT id FROM pim_product WHERE model_id IN (SELECT id FROM pim_model WHERE model_number=" + row["model_number"] + ") AND brand_size_id IN (SELECT id FROM pim_brand_size WHERE name LIKE '%" + str(row["size"]) + "%')"
			self.mycursor.execute(sql)
			myresult = self.mycursor.fetchall()
			for result in range(len(myresult)):
				
				sql = "INSERT INTO pim_sizeguide_product (product_id, "+','.join(self.columnsMens)+", created_at, updated_at) VALUES (%s, %s, %s"
				val = [myresult[result][0]]
				if sql_class.productidChecker(val) == False:
					
					for men in self.columnsMens:
						sql += ',%s'
						val.append(row[men])
					val.append(strftime("%H-%m-%d %H:%M:%S", gmtime()))
					val.append(strftime("%H-%m-%d %H:%M:%S", gmtime()))
					val = tuple(val)
					sql += ')'
					try:

						self.mycursor.execute(sql, val)
						self.mydb.commit()
						print('send to db successful')

					except Exception as e:
						print(e)
						print('send to db not successful')
						logging.error("Data from the file {} failed the export to DB".format(self.file))

	def exec_timer(self):
		timing = time.time() - self.start_time
		rounded_seconds = round(timing, 2)
		rounded_minutes = round(timing/60, 2)
		print("Script executed in " + str(rounded_seconds) + " seconds")
		print("Script executed in " + str(rounded_minutes) + " minutes")
		logging.info("Script executed in " + str(rounded_seconds) + " minutes")
		logging.info("Script executed in " + str(rounded_minutes) + " minutes")

	def ender(self):
		# print('number of records successulf : ' + str(self.success_counter))
		# print('number of records NOT successulf : ' + str(self.failed_counter))
		# for i in self.failed_number:
		# 	print('-------------------------')
		# 	print(repr(i))
		# 	print('-------------------------')
		with open("failed_files.txt", "a", newline='\n') as fp:
			for i in self.failed_file:
				#fp.write('---------------------------' + '\n')
				fp.write(i + '\n')
		sql_class.exec_timer()
		print('Script successfully ended')
		logging.info('Script ended gracefully')

if __name__ == '__main__':
	sql_class = Sql()
	sql_class.logger()
	sql_class.connectorwamp()
	sql_class.databaseMaxRows()
	sql_class.remove_hidden_rows()
	sql_class.clean_data()
	sql_class.ender()