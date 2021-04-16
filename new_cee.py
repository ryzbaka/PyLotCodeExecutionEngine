import sys
import os
import pickle
import pandas as pd
from flask import Flask,request,jsonify
from inspect import getfullargspec
import sqlite3
import pandas as pd
HOME_PATH = os.path.abspath(".")
PORT = 5200
MAX_ROWS = 20 #maximum number of rows that get sent to front-end for display.
app = Flask(__name__)


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

def checkFileSystemValid(notebookName,tileName):
    '''
    This function checks if the notebook/tile name is valid and present in the DB for execution.
    '''
    pass
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
            if(len(main_args)==0):
                result = main()
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
                    except Exception as e:
                        return jsonify({"error":"[Error saving output to local database]: "+str(e)})

                    return jsonify(result.head(MAX_ROWS).to_dict())
            else:
                '''
                logic for tiles that accept 
                '''
                return jsonify({"message":"logic for tiles that take other tiles as input is being developed still."})
        except Exception as e:
            return jsonify({"error":str(e)})
    else:
        return jsonify({"error":"Code for tile not found or (Notebook/tile) does not exist."})
if __name__=='__main__': 
    app.run(port = PORT, debug=True)
