import pandas as pd
import pyodbc

# sqlserver connection
conn = pyodbc.connect('Driver={SQL Server};'
                        'Server=LAPTOP-4ECIVIDK\SQLEXPRESS;'
                        'Database=PF2e_Bestiary;'
                        'Trusted_Connection=yes;')
cursor = conn.cursor()

# pull data from Archives of Nethys
df = pd.read_html('https://2e.aonprd.com/Monsters.aspx?Letter=All')

# convert raw data into a DataFrame
monsters = pd.DataFrame(df[0])

# create lists of unique data for each column (except Name)
uniqueFamily = pd.DataFrame(monsters['Family'].unique())
uniqueLevel = pd.DataFrame(monsters['Level'].unique())
uniqueAlignment = pd.DataFrame(monsters['Alignment'].unique())
uniqueCreatureType = pd.DataFrame(monsters['Creature Type'].unique())
uniqueSize = pd.DataFrame(monsters['Size'].unique())

# sort unique lists
uniqueFamily = uniqueFamily.sort_values(by=[0])
uniqueLevel = uniqueLevel.sort_values(by=[0])
uniqueAlignment = uniqueAlignment.sort_values(by=[0])
uniqueCreatureType = uniqueCreatureType.sort_values(by=[0])
uniqueSize = uniqueSize.sort_values(by=[0])

# populate monster table
# define columns for data to be inserted into
cols = ",".join([str(i).replace(" ","") for i in monsters.columns.tolist()])

#insert records one by one
for i,row in monsters.iterrows():
    sql = "INSERT INTO dbo.monster (" + cols + ") VALUES (?,?,?,?,?,?)"
    cursor.execute(sql, tuple(row))

    conn.commit()

# populate family table
for i, row in uniqueFamily.iterrows():
    sql = "INSERT INTO dbo.family (family) VALUES (?)"
    cursor.execute(sql, tuple(row))

    conn.commit()

# populate level table
for i, row in uniqueLevel.iterrows():
    sql = "INSERT INTO dbo.level (level) VALUES (?)"
    cursor.execute(sql, tuple(row))
    
    conn.commit()

# populate alignment table
for i, row in uniqueAlignment.iterrows():
    sql = "INSERT INTO dbo.alignment (alignment) VALUES (?)"
    cursor.execute(sql, tuple(row))
    
    conn.commit()

# populate creatureType table
for i, row in uniqueCreatureType.iterrows():
    sql = "INSERT INTO dbo.creatureType (creatureType) VALUES (?)"
    cursor.execute(sql, tuple(row))

    conn.commit()

# populate size table
for i, row in uniqueSize.iterrows():
    sql = "INSERT INTO dbo.size (size) VALUES (?)"
    cursor.execute(sql, tuple(row))
    
    conn.commit()

# close database connection
conn.close()