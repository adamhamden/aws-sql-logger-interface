#!/usr/bin/env python3

import mysql.connector

mydb = mysql.connector.connect(
  host="database-2.csozaa62oxle.us-east-1.rds.amazonaws.com",
  port = 3306,
  user="masterUsername",
  passwd="adamhamden",
)
print(mydb)
#mycursor = mydb.cursor()

#mycursor.execute("CREATE DATABASE mydatabase")