import os
import pickle
import pandas as pd
from importlib import import_module

HOME_PATH = os.path.abspath(".")

def checkFileSystemValid(notebookName,tileName):
    try:
        tile_path = f"notebooks/{notebookName}/{tileName}"
        os.chdir(tile_path)
        os.chdir(HOME_PATH)
        return tile_path
    except:
        return False

def execute(notebookName,tileName):
    #Read input as pandas dataframe
    os.chdir(HOME_PATH) # move to home path
    fsValid = checkFileSystemValid(notebookName,tileName)
    if fsValid:
        os.chdir(fsValid) # fsValid returns the path of the tile if exists, else return False
        information = None 
        with open("info.pickle","rb") as f: 
            #expects "info.pickle" to already exist in tile folder
            #modify DAG to FS replication code to add "info.pickle" files
            #in the apropriate folders.
            information = pickle.load(f)
        os.chdir(HOME_PATH)#switching to root folder for execution.
    #Importing tile code as module and executing.
        if information["inputTile"]==None:
            #input type = "dataset"
            code = import_module(f"notebooks.{notebookName}.{tileName}.code")
            result = code.main() #get dataframe result of tile.
            if type(result) != type(pd.DataFrame()):
                print(f"PyLot : [Error in main] Invalid function output type, expected Pandas dataframe got {type(result)}.")
            else:
    #Writing tile code output to csv
                os.chdir(fsValid)
                result.to_csv("output/output.csv")
                print("done!")
        os.chdir(HOME_PATH) #return to home path (root directory for code execution engine)
                
    else:
        print("Invalid notebook or tile name.")
if __name__=='__main__':
    notebookName = "notebook1"#input("Enter name of notebook >")
    tileName = "tile1"#input("Enter name of tile >")
    print(f"Executing {notebookName}'s tile: {tileName} ...")
    execute(notebookName,tileName)