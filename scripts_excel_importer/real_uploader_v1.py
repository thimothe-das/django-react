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
			user="phpasmopim",
			passwd="changeme",
			database="dbasmodinepim",
			port=3332
			)
		self.mycursor = self.mydb.cursor()
		self.table_name = "pim_brand_size"


	def getSizesBrand(self):
		sql = "SELECT name FROM pim_brand_size"
		self.mycursor.execute(sql)
		myresult = self.mycursor.fetchall()
		sizes = [item[0] for item in myresult]
		return sizes

	def remove_hidden_rows(self):
		self.files = [f for f in listdir('./files/') if isfile(join('./files/', f))]
		for self.file in self.files:
			print(self.file)
			try:
				wb = xlrd.open_workbook('./files/' + self.file, formatting_info=1)
			except Exception as e:
				print('xlxs file, should be xls file')
				file_error = 'FILE : ' + str(self.file) + ' ERROR CODE : ' + str(
					e) + ' ERREUR : ' + 'Wrong file extension, should be xls (not xlxs)'
				self.failed_file.append(file_error)
				with open("failed_files" + ".csv", "a", newline='\n') as fp:
					a = csv.writer(fp)
					row = [str(self.file)] + ['Wrong file extension, should be xls (not xlxs)']
					a.writerow(row)
				copyfile("files/" + self.file, "failed_files/xlxsFiles/" + self.file)

			wb_sheet = wb.sheet_by_index(0)

			newBook = xlwt.Workbook()
			newSheet = newBook.add_sheet("data")

			idx = 0
			try:
				for row_idx in range(0, wb_sheet.nrows):
					hidden = wb_sheet.rowinfo_map[row_idx].hidden
					if (hidden == 0):
						for col_index, cell_obj in enumerate(wb_sheet.row(row_idx)):
							newSheet.write(idx, col_index, cell_obj.value)
						idx = idx + 1
				newBook.save('./files/noHiddenRows/' + self.file)
			except:
				copyfile("files/" + self.file, "failed_files/wentWrong/" + self.file)

				print("Something went wrong")

	def clean_data(self):
		self.mycursor.execute("TRUNCATE TABLE pim_sizeguide_product")
		with open("failed_files" + ".csv", "w", newline='\n') as fp:
			a = csv.writer(fp)
			row = ['FILE NAME'] + ['ERROR EXPLAINED']
			a.writerow(row)
		dfCodes = pd.read_csv("codeName.csv")
		self.files = [f for f in listdir('./files/noHiddenRows/') if isfile(join('./files/noHiddenRows/', f))]
		for self.file in self.files:
			self.columnsMens = []
			print(self.file)

			self.match = re.search(r'\d{7}', self.file)
			sizesBrand = self.getSizesBrand()
			df = pd.read_excel('./files/noHiddenRows/' + self.file, sheet_name='data', header=1)
			self.df = df
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
				else:
					self.df = df
					sql_class.sql_uploader()
					print(self.df)
			except:
				copyfile("files/" + self.file, "failed_files/wentWrong/" + self.file)
				print("Something went wrong")


	def getAllCodeNames(self):
		dfCodeNames = pd.read_csv("codeName.csv")
		allCodenames = dfCodeNames["code"].tolist()
		return allCodenames


	def sql_uploader(self):
		for index, row in self.df.iterrows():
			sql = "SELECT id FROM pim_product WHERE model_id IN (SELECT id FROM pim_model WHERE model_number=" + row["model_number"] + ") AND brand_size_id IN (SELECT id FROM pim_brand_size WHERE name LIKE '%" + str(row["size"]) + "%')"
			self.mycursor.execute(sql)
			myresult = self.mycursor.fetchall()
			for result in range(len(myresult)):

				sql = "INSERT INTO pim_sizeguide_product (product_id, "+','.join(self.columnsMens)+", created_at, updated_at) VALUES (%s, %s, %s"
				val = [myresult[result][0]]
				for men in self.columnsMens:
					sql += ',%s'
					val.append(row[men])
				val.append('2019-12-23')
				val.append('2019-12-23')
				val = tuple(val)
				sql += ')'
				try:
					self.mycursor.execute(sql, val)
					self.mydb.commit()
					print('send to db successful')
				except:
					print('send to db not successful')


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
		print('Script successfully ended')

if __name__ == '__main__':
	sql_class = Sql()
	sql_class.connectorwamp()
	#sql_class.remove_hidden_rows()
	sql_class.clean_data()
	sql_class.ender()