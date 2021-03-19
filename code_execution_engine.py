import os
from sys import argv
from flask import Flask,request,jsonify
from importlib import import_module
from replicate import replicateToFileSystem
home = os.path.abspath(".")
app = Flask(__name__)
PORT = 5200

def checkFileSystemValid(notebookName,tileName):
    try:
        os.chdir(f"notebooks/{notebookName}/{tileName}")
        return True
    except:
        return False

@app.route("/checkOnline",methods=["GET"])
def checkOnline():
    return jsonify({"message":1})

@app.route("/replicateNotebook",methods=["POST"])
def replicateNotebook():
    # print(replicateToFileSystem(request.json["notebook"], request.json["user"]))
    username = request.json["user"]
    notebook = request.json["notebook"]
    print(replicateToFileSystem(notebook, username))
    return jsonify({"message":"Python code execution engine received notebook object"})

@app.route("/runTile",methods=["POST"])
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
    os.chdir(home)
    if fileSystemValid:
        full_module_name = f"notebooks.{notebookName}.{tileName}.code"
        module = import_module(full_module_name)
        # try: 
        resultant = module.main() #resultant will store output of main function
        try:
            return jsonify(resultant.to_dict())
        except:
            return jsonify({"message":"Error."})
        # except:
            # return "Error ocurred while executing code."

    else:
        return jsonify({"message":"Notebook/Tile not found. (Invalid name / stale data?)"})


if __name__=='__main__': 
    app.run(port = PORT, debug=True)