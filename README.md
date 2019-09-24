<a href="https://imgflip.com/gif/3bd8pz"><img src="https://i.imgflip.com/3bd8pz.gif" title="made at imgflip.com"/></a>

**Version 0.1**

Script converts multiple .dbf files into a single sqlite table for querying and exporting transformed files to csv. 



**Intended Purpose:** I wrote this script primarily for migrating legacy FoxPro database based apps. These apps normally have mass amounts of .dbf files scattered over multitple directories. I use MacOS's search function to find all .dbf inside of the root directory and then just copy and paste into the data folder.



**Usage:** Place all .dbf files into the data folder execute main.py with Python3 in your console. Select y when asked if you want to load database. The script will list out all files it's processing, once complete it will export any csv files to the export directory if a function call was initated. There is also a index function that allows you to easily create indexes on large tables durning database creation.

The csv function takes a query string and file name and exports a csv. You may also specify a split_count which will split the file into chunks the size of the count specififed.



For querying the database I recommend: https://sqlitebrowser.org/ 
