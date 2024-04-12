Project done by Daniel Guo, Student Number 101228932

As stated in the project report, the steps to run the database creation are as follows:
1. Clone this repo and add the statsbomb daata file from https://github.com/statsbomb/open-data/tree/0067cae166a56aa80b2ef18f61e16158d6a7359a to the repo folder
2. Run the python file format_jsons.py to sort and filter all relevant match, team, player, etc. data from the statbomb file
3. Run the python file INSERT_INTO_TABLES.py to load all the data onto the specified database project_database on port 5432 as user postgres with password 1234 in localhost

After creating the database, queries.py is able to be run on a linux machine to verify the query correctness.

PLEASE NOTE: For testing, I was unable to access a linux machine. Because of this, queries were verified locally on my Win11 machine with pgAdmin4 using the query tool. Output was then manually confirmed
PLEASE NOTE: Bonus queries are included in the porject report, both as a video showcase and text. They are not available in the queries.py file
