# # 1, Import libraries:
import mysql.connector
from mysql.connector import Error
#
#
# # __________________________________________
#
#
# # 2. Connects to the MySQL database server:
def create_connection(host_name, user_name, user_password,db_name, unix_socket):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name,
            unix_socket=unix_socket
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection


connection = create_connection("localhost", "root", "root", "sm_app","/Applications/MAMP/tmp/mysql/mysql.sock")


# __________________________________________


# 3. Create the database:
def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as e:
        print(f"The error '{e}' occurred")


create_database_query = "CREATE DATABASE sm_app"
create_database(connection, create_database_query)


# __________________________________________

#
# # 4. Create function to excute queries:
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")


# # __________________________________________
#
#
# # 5. Create queries for creating tables:
create_employees_table = """
CREATE TABLE IF NOT EXISTS employees (
  id INT AUTO_INCREMENT,
  name TEXT NOT NULL,
  age INT,
  gender TEXT,
  nationality TEXT,
  PRIMARY KEY (id)
) ENGINE = InnoDB
"""

create_position_table = """
CREATE TABLE IF NOT EXISTS position (
  id INT AUTO_INCREMENT,
  name TEXT NOT NULL,
  salary TEXT NOT NULL,
  PRIMARY KEY (id)
) ENGINE = InnoDB
"""
create_EmpPosition_table = """
CREATE TABLE IF NOT EXISTS EmpPosition (
  id INT AUTO_INCREMENT,
  emp_id INT NOT NULL,
  pos_id INT NOT NULL,
  FOREIGN KEY fk_emp_id (emp_id) REFERENCES employees (id),
  FOREIGN KEY fk_pos_id (pos_id) REFERENCES position  (id),
  PRIMARY KEY (id)
) ENGINE = InnoDB
"""
#
create_department_table = """
CREATE TABLE IF NOT EXISTS department (
  id INT AUTO_INCREMENT,
  name TEXT NOT NULL,
  description TEXT NOT NULL,
  PRIMARY KEY (id)
) ENGINE = InnoDB
"""
create_empDepartment_table = """
CREATE TABLE IF NOT EXISTS empDepartment (
  id INT AUTO_INCREMENT,
  emp_id INT NOT NULL,
  dep_id INT NOT NULL,
  FOREIGN KEY fk_emp_id (emp_id) REFERENCES employees (id),
  FOREIGN KEY fk_dep_id (dep_id) REFERENCES department (id),
  PRIMARY KEY (id)
) ENGINE = InnoDB
"""
create_empRate_table = """
CREATE TABLE IF NOT EXISTS empRate (
  id INT AUTO_INCREMENT,
  emp_id INT NOT NULL,
  rate INT NOT NULL,
FOREIGN KEY fk_emp_id (emp_id) REFERENCES employees (id),
  PRIMARY KEY (id)
) ENGINE = InnoDB
"""
execute_query(connection, create_employees_table)
execute_query(connection, create_position_table)
execute_query(connection, create_EmpPosition_table)
execute_query(connection, create_department_table)
execute_query(connection, create_empDepartment_table)
execute_query(connection, create_empRate_table)


# # # __________________________________________
# #
# #
# # # # 6. Create INSERT queries:
create_employees_query = "INSERT INTO employees (name, age, gender, nationality) VALUES (%s, %s, %s, %s)"
emp_val = [('James', 25, 'male', 'USA'),
           ('Leila', 32, 'female', 'France'),
           ('Brigitte', 35, 'female', 'England')]


cursor = connection.cursor()
cursor.executemany(create_employees_query, emp_val)
connection.commit()
# # __________________________________________

create_position_query = "INSERT INTO position (name,salary) VALUES (%s, %s)"
pos_val = [('James', 2500),
           ('Leila', 3200),
           ('Brigitte', 35000)]

cursor = connection.cursor()
cursor.executemany(create_position_query, pos_val)
connection.commit()
# # __________________________________________

create_EmpPosition_query = "INSERT INTO EmpPosition (emp_id ,pos_id) VALUES (%s, %s)"
emppo_val = [(1, 1),
             (2, 2),
             (3, 3)]

cursor = connection.cursor()
cursor.executemany(create_EmpPosition_query, emppo_val)
connection.commit()
# # __________________________________________

create_department_query = "INSERT INTO department (name ,description) VALUES (%s, %s)"
dep_val = [('lama', 'it department'),
             ('amal', 'HR'),
             ('ahmed', 'sales')]

cursor = connection.cursor()
cursor.executemany(create_department_query, dep_val)
connection.commit()
# # __________________________________________

create_empDepartment_query = "INSERT INTO empDepartment (emp_id ,dep_id) VALUES (%s, %s)"
empdep_val = [(1, 1),
             (2, 2),
             (3, 3)]

cursor = connection.cursor()
cursor.executemany(create_empDepartment_query, empdep_val)
connection.commit()
# ---------------------------------------------------------
create_empRate_query = "INSERT INTO empRate (emp_id ,rate) VALUES (%s, %s)"
emprate_val = [(1, 5),
              (2, 4),
              (3, 9)]

cursor = connection.cursor()
cursor.executemany(create_empRate_query, emprate_val)
connection.commit()

# ------------------------------------------------------------------
def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")
#
#

# # ___________QUERIES _______________________________

# 1.SELECT queries:

select_position = "SELECT * FROM position"
postions = execute_read_query(connection, select_position)

for pos in postions:
    print(pos)

# # __________________________________________
#2.Select using where

select_emp = "SELECT * FROM employees WHERE id = 2"
emps = execute_read_query(connection, select_emp)

for em in emps:
    print(em)

# # __________________________________________
#
#3. Select using JOIN

Select_empPos = """SELECT employees.id  FROM employees
JOIN EmpPosition ON employees.id = EmpPosition.pos_id"""
empPosition = execute_read_query(connection, Select_empPos)

for emPos in empPosition:
    print(emPos)

# # __________________________________________
#
#4. Update queries:
update_empRate = """
UPDATE
  empRate
SET
  rate = 10
WHERE
  id = 2 
"""

execute_query(connection,  update_empRate)


# # __________________________________________
#
#
#5. Delete queries:
delete_department = "DELETE FROM employees WHERE id = 1 "
execute_query(connection, delete_department)

