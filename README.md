Requirements:
To run the program, please make sure python3 is installed in your platform, and some relative modules (For example: pandas, numpy, pyparsing and so on) are installed in your platform. 

How to run the program:
1. Please make sure to run this program on the same directory with your database's directory. For example, if we have a directory "C:\\UIUC\\simpleDb" and we have two csv files A.csv and B.csv in the directory simpleDb, before running the program, please put the python files to the UIUC directory.
2. Use python3 command to run file run.py 
3. In command line, input import + the directory name as the database name. In our example, type in "import simpleDb", then A.csv and B.csv will be loaded to the program.
4. In command line, input Q + the query, and wait for the results. (Our program will check Q as the identifier to start the query)
5. In command line, input exit() to quit the program.

Command format:
1. To use the inner join, please explicit use the "join" and "on" keywords, and please add a pair of buckets for the condition(s) even though there is only one condition. We don't support implicit format. For example, if we want to join table A and table B on A.a and B.b, the sql should be: "SELECT * FROM A JOIN B ON (A.a=B.b)", and should not be: "SELECT * FROM A, B WHERE A.a=B.b". We may support the implicit approach in the final version.
2. We use != replace <> to check for inequality
3. A comma is needed for the last from statement. For example, "SELECT * FROM A, B WHERE A.a=B.b" should be written as "SELECT * FROM A, B WHERE A.a=B.b," in our program. We will modify it to fit with the standard syntax in the final version.
