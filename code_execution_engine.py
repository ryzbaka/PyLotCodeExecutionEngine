import os
import pickle
import pandas as pd
from sys import argv
from flask import Flask,request,jsonify
from importlib import import_module
from replicate import replicateToFileSystem

HOME_PATH = os.path.abspath(".")
app = Flask(__name__)
PORT = 5200


@app.route("/checkOnline",methods=["GET"]) # maybe remove this.
def checkOnline():
    return jsonify({"message":1})

# def checkFileSystemValid(notebookName,tileName):
#     try:
#         os.chdir(f"notebooks/{notebookName}/{tileName}")
#         return True
#     except:
#         return False
def checkFileSystemValid(notebookName,tileName):
    os.chdir(HOME_PATH)
    try:
        tile_path = f"notebooks/{notebookName}/{tileName}"
        os.chdir(tile_path)
        os.chdir(HOME_PATH)
        return tile_path
    except:
        return False


@app.route("/replicateNotebook",methods=["POST"])
def replicateNotebook():
    '''
    This function is used for handling the replication of a PyLot Notebook object to the user's filesystem.
    '''
    username = request.json["user"]
    notebook = request.json["notebook"]
    print(replicateToFileSystem(notebook, username))
    return jsonify({"message":"Python code execution engine received notebook object"})

@app.route("/runTile",methods=["POST"])#this function needs redoing since it doesn't factor in input and output tiles. You'll have to think about this
def runTile():
    '''
    This function currently returns the output pandas dataframe as json
    '''
    try:
        notebookName = request.json["notebookName"]
        tileName  = request.json["tileName"]
    except:
        return jsonify({"message":"notebookName or tileName not specified"})
    fileSystemValid = checkFileSystemValid(notebookName, tileName)
    os.chdir(HOME_PATH)
    if fileSystemValid:
        os.chdir(HOME_PATH)
        os.chdir(fileSystemValid)
        information = None
        with open("info.pickle","rb") as f:
            information = pickle.load(f)
        os.chdir(HOME_PATH)
        if information["inputTile"]==None:
            #input type = "dataset"
            try:
                code = import_module(f"notebooks.{notebookName}.{tileName}.code")
                result = code.main()
            except Exception as e:
                errorString = str(e)    
                return jsonify({"message":errorString})
            # print(f"result: {result}")
            if type(result)!=type(pd.DataFrame()):
                return jsonify({
                    "message":f"PyLot : [Error in main] Invalid function output type, expected Pandas dataframe got {type(result)}."
                })
            else:
                os.chdir(fileSystemValid)
                result.to_csv("output/output.csv")
        else:
            #input type : Tile
            pass
        os.chdir(HOME_PATH)        
        return jsonify(result.to_dict())     
    else:
        return jsonify({"message":"Notebook/Tile not found. (Invalid name / stale data?)"})


if __name__=='__main__': 
    app.run(port = PORT, debug=True)
