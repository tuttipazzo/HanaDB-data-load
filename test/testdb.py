from hanaDatabase import *
import traceback


if __name__ == "__main__":
    tableName = "Contacts"
    address = "localhost"
    port=30015
    user='system'
    passwd='YourPassword1234'
#    createStmt = """CREATE TABLE IF NOT EXISTS {0} (
    createStmt = """CREATE TABLE {0} (
                            fName   VARCHAR(5000),
                            lName   VARCHAR(5000),
                            address VARCHAR(5000),
                            city    VARCHAR(5000),
                            state   VARCHAR(5000),
                            zipCode integer);""".format(tableName)
    db = None 
    try:
        db = database(address, port, tableName, createStmt, user, passwd, saccess=True, debug=True)
    except Exception as e:
        print("ERROR: {0}\\n{1}".format(e, traceback.format_exc()))
	exit

    print("********************\nADDING records\n**********************")
    add1 = ["Joanna", "Strange", "1234 main st.", "Stockton", "CA", 92880]
    add2 = ["John", "Smith", "1234 main st.", "Eastvale", "CA", 92880]
    add3 = ["Jane", "Butts", "1234 main st.", "Eastvale", "CA", 92880]
    db.add(add1)
    db.add(add2)
    db.add(add3)

    recs = db.getAllRows()
    for i in recs:
        print(i)

    print("\n********************\nUpdating records\n**********************")

    newadd1 = ["Patty", "Smith", "1234 main st.", "Eastvale", "CA", 92880]
    db.update(add1, newadd1)

    recs = db.getAllRows()
    for i in recs:
        print(i)

    print("\n********************\nDelete record\n**********************")
    db.delete(add2)
    recs = db.getAllRows()
    for i in recs:
        print(i)

