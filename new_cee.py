from datetime import datetime
import sys
import os
import sqlite3
import pandas as pd
from flask import Flask,request,jsonify
from inspect import getfullargspec
from apscheduler.schedulers.background import BackgroundScheduler

HOME_PATH = os.path.abspath(".")
PORT = 5200
MAX_ROWS = 20 #maximum number of rows that get sent to front-end for display.

app = Flask(__name__)
sched = BackgroundScheduler(daemon=True)
sched.start()

def replicateToDatabase(notebook):
    '''
    replicate notebook object to database. REMEMBER NOTEBOOKNAMES SHOULD BE PURELY ALPHANUM
    '''
    conn = sqlite3.connect("pylot.db") #starting connection.
    notebookName = notebook["name"]
    tiles = notebook["tiles"] #[{"information":{"name","inputTileNames","code"}}]
    tileNames = notebook["tileNames"]
    #Creating dedicated table for notebook if it does not exist.
    try:
        conn.execute(f"drop table if exists {notebookName}")
        conn.commit()
        conn.execute(f"create table {notebookName}(tileName text,code text,inputTiles text)")
        conn.commit()
    except Exception as e:
        print("[Error in replicatetoDB table creation]: "+str(e))
        conn.close()
        return jsonify({"error":"[Error in replicatetoDB table creation]: "+str(e)})
    #End of table creation
    for idx,tile in enumerate(tiles):
        info = tile["information"]
        print(info)
        code = info["code"]
        name = info["name"]
        inputTileNames = info["inputTileNames"]
        inputTileNames = ",".join(inputTileNames) #array of input tiles is stores as comma separated strings.
        # print(info)
        print(inputTileNames)
        try:
            #creating creating entries in the table for each of the tiles.
            if code == "":
                code = "empty"
            print(f"{name}:{code}")
            conn.execute(f"insert into {notebookName} values (?,?,?)",(name,code,inputTileNames))
            conn.commit()
        except Exception as e:
            print(f"[Error while inserting {name} in {notebookName}]: "+str(e))
            conn.close()
            return jsonify({"error":f"[Error while inserting {name} in {notebookName}]: "+str(e)})
    conn.close() # closing connection
    return jsonify({"message":"under-construction"})

# @app.route("/fetchCode",methods=["POST"])
def fetchCode(notebookName,tileName):
    # notebookName = request.json["notebook"]
    # tileName = request.json["tile"]
    try:
        conn = sqlite3.connect("pylot.db")
        result = conn.execute(f"select code from {notebookName} where tileName = '{tileName}'").fetchall()
        if len(result)==0:
            print("Tile does not exist in notebook")
            return False
        code = result[0][0]
        return code
    except Exception as e:
        print(str(e))
        return False

@app.route("/checkOnline",methods=["GET"]) # maybe remove this.
def checkOnline():
    return jsonify({"message":1})

@app.route("/replicateNotebook",methods=["POST"])
def replicateNotebook():
    '''
    This function is used for handling the replication of a PyLot Notebook object to the user's sqlite database.
    '''
    notebook = request.json["notebook"]
    return replicateToDatabase(notebook)
    
@app.route("/runTile",methods=["POST"])
def runTile():
    '''
    This function retrieves tile information from database and runs them.
    '''
    notebookName = request.json["notebookName"]
    tileName = request.json["tileName"]
    code = fetchCode(notebookName,tileName)
    if code:
        try:
            #executing code
            env = {}
            obj = compile(code,"","exec")
            exec(obj,env)
            main = env["main"]
            # print(getfullargspec(main).args)
            main_args = getfullargspec(main).args
        except Exception as e:
            return jsonify({"error":f"[Error while accessing main() from tile: {tileName}'s' code] : "+str(e)})
        
        if(len(main_args)==0):
            '''
            logic for tiles that read from dataset or have no input.
            '''
            try:
                result = main()
            except Exception as e:
                return jsonify({"error":f"[Error while executing main() from tile :{tileName}'s code'] : "+str(e)})
            if type(result)!=type(pd.DataFrame()):
                return jsonify({
                    "message":f"PyLot : [Error in main] Invalid function output type, expected Pandas dataframe got {type(result)}."
                })
            else:
                try:
                    '''
                    uploading output to database
                    '''
                    conn = sqlite3.connect("pylot.db")
                    result.to_sql(tileName+"_"+notebookName,conn,if_exists="replace",index=False)
                    conn.close()
                except Exception as e:
                    return jsonify({"error":"[Error saving output to local database]: "+str(e)})

                return jsonify(result.head(MAX_ROWS).to_dict())
        else:
            '''
            logic for tiles that accept other tiles as input.
            Note: The formal arguments in these tiles should match the name of the tile they wish to use as input so as to avoid ambiguity.
            '''
            # print(main)
            print(main_args)
            #retrieve required data from database
            main_dfs=[]
            for arg in main_args:
                data = fetchData(notebookName,arg)
                if type(data) == type(pd.DataFrame()):
                    main_dfs.append(data)
                else:
                    return jsonify({"error":f"couldn't fetch data ({arg}'s output) required to run tile code."})
            try:
                result = main(*main_dfs)
            except Exception as e:
                return jsonify({"error":f"[Error while executing {tileName}'s code'] : "+str(e)})
            try:
                '''
                Uploading output to database.
                '''          
                conn = sqlite3.connect("pylot.db")
                result.to_sql(tileName+"_"+notebookName,conn,if_exists="replace",index=True) #maybe change index to True
            except Exception as e:
                return jsonify({"error":"[Error while saving code output to local database]"})
            return jsonify(result.head(MAX_ROWS).to_dict())
            # return jsonify({"message":"logic for tiles that take other tiles as input is being developed still."})
    else:
        return jsonify({"error":"Code for tile not found or (Notebook/tile) does not exist."})

@app.route("/fetchOutput",methods=["POST"])
def fetchOutput():
    notebookName = request.json["notebookName"]
    tileName = request.json["tileName"]
    tableName = f"{tileName}_{notebookName}"
    try:
        conn = sqlite3.connect("pylot.db")
        data = pd.read_sql_query(f"select * from {tableName}",conn)
        conn.close()
        return jsonify(data.head(MAX_ROWS).to_dict())
    except Exception as e:
        print(str(e))
        return jsonify({"error":str(e)})

def test_schedule_function(username):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"{username} the time is {now}")

# @app.route("/runDAG",methods=["POST"])
def run_scheduled_dag(notebookName):
    #convert all returns to logs
    # notebookName = request.json["notebookName"]
    try:
        conn = sqlite3.connect("pylot.db")
        data = pd.read_sql_query(f"select * from {notebookName}",conn)
        data_dict = data.to_dict()
        code_snippets = list(data_dict["code"].values())
    # print(code_snippets)
    except Exception as e:
        print(str(e))
    for index,code in enumerate(code_snippets):
        # print(data_dict["tileName"][str(index)])
        tileName = data_dict["tileName"][index]
        print(f"Executing Notebook: {notebookName} ; tile: {tileName} ; {datetime.now().strftime('%H:%M:%S')}")
        try:
            env = {}
            obj = compile(code,"","exec")
            exec(obj,env)
            main = env["main"]
            main_args = getfullargspec(main).args
        except Exception as e:
            print({"error":f"[Error while accessing main()]"})
        if len(main_args)==0:
            #tile has no input
            try:
                result = main()
            except Exception as e:
                print({"error":f"[Error executing main()]"})
            if type(result)!=type(pd.DataFrame()):
                print({"error":f"[Error in main] Invalid function output type"})
            try:
                #saving to sqlite
                # tileName = data_dict["tileName"][index]
                conn = sqlite3.connect("pylot.db")
                result.to_sql(tileName+"_"+notebookName,conn,if_exists="replace",index=False)
                conn.close()
            except Exception as e:
                print({"error":f"[Error while saving output]"})
        else:
            #tile has input
            main_dfs=[]
            for arg in main_args:
                data = fetchData(notebookName,arg)
                if type(data) == type(pd.DataFrame()):
                    main_dfs.append(data)
                else:
                    print({"error":f"couldn't fetch data ({arg}'s output) required to run tile code."})
            try:
                result = main(*main_dfs)
            except Exception as e:
                print({"error":f"[Error while executing {tileName}'s code'] : "+str(e)})
            try:
                '''
                Uploading output to database.
                '''          
                conn = sqlite3.connect("pylot.db")
                result.to_sql(tileName+"_"+notebookName,conn,if_exists="replace",index=True) #maybe change index to True
            except Exception as e:
                print({"error":"[Error while saving code output to local database]"})
    print({"message":"done"})
@app.route("/scheduleNotebook",methods=["POST"])
def scheduleDAG():
    '''
    This function is currently being used to test function scheduling.

    Write code here to run a dag.
    '''
    notebook = request.json["notebookName"]
    minutes = request.json["minutes"]
    sched.add_job(run_scheduled_dag,'interval',minutes=minutes,id=notebook,args=[notebook])
    print(f"Scheduled notebook {notebook} to run every {minutes} minute(s).")
    return "Done scheduling"

@app.route("/getJobList",methods=["POST"])
def getJobs():
    sched.print_jobs()
    return jsonify({"message":"done"})

@app.route("/removeJob",methods=["POST"])
def removeJob():
    job_id = request.json["notebook"]
    try:
        sched.remove_job(job_id)
        return jsonify({"message":"Stopped job successfully."})
    except Exception as e:
        return jsonify({"message":str(e)})
def fetchData(notebookName,tileName):
    tableName = f"{tileName}_{notebookName}"
    try:
        conn = sqlite3.connect("pylot.db")
        data = pd.read_sql_query(f"select * from {tableName}",conn)
        conn.close()
        return data
    except Exception as e:
        print(str(e))
        return False

if __name__=='__main__': 
    app.run(port = PORT, debug=True)