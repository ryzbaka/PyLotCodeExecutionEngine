"   MAYBE TRY READING WITH 'R' INSTEAD OF 'RB'" 
import sys
import os
import pickle
import pandas as pd
from flask import Flask,request,jsonify
from inspect import getfullargspec
HOME_PATH = os.path.abspath(".")
app = Flask(__name__)
PORT = 5200



def replicate(notebook):
    notebookName = notebook["name"]
    tiles = notebook["tiles"]
    tileNames = notebook["tileNames"]
    home = os.path.abspath(os.curdir)
    if ("notebooks" not in os.listdir()) and ("datasets" not in os.listdir()):
        print(os.listdir())
        fsInit()
    os.chdir("notebooks")
    if notebookName not in os.listdir():
        #fresh notebook creation
        print("~FRESH NOTEBOOK CREATION~")
        os.mkdir(notebookName)
        os.chdir(notebookName)
        for idx,tileName in enumerate(tileNames):
            os.mkdir(tileName)
            os.chdir(tileName)
            os.mkdir("output")
            inputTile = None
            if len(tiles[idx]["information"]["inputTileNames"])>0:
                inputTile = tiles[idx]["information"]["inputTileNames"]
            information = {
                "name":tileName,
                "inputTile":inputTile
            }
            # print(information)
            with open("info.pickle","wb") as f: #!!!!!!!
                pickle.dump(information,f)
            # print(tiles[idx]["information"]["code"])
            with open("code.py","w") as f:
                f.write(tiles[idx]["information"]["code"])
            os.chdir("..")
    else:
        print("~MODIFYING EXISTING NOTEBOOK~")
        os.chdir(notebookName)
        #delete tiles that arent in the notebook anymore
        existingTileNames = os.listdir()
        for name in existingTileNames:
            if name not in tileNames:
                rmtree(name,ignore_errors=True)
        #add tiles that arent in the fs
        for idx,name in enumerate(tileNames):
            if name not in os.listdir():
                os.mkdir(name)
                os.chdir(name)
                os.mkdir("output")
                inputTile = None
                if len(tiles[idx]["information"]["inputTileNames"]) > 0:
                    inputTile = tiles[idx]["information"]["inputTileNames"]
                information = {
                    "name":name,
                    "inputTile":inputTile
                }
                with open("info.pickle","wb") as f:
                    pickle.dump(information,f)
                with open("code.py","w") as codeObject:
                    codeObject.write(tiles[idx]["information"]["code"])
            else:
                os.chdir(name)
                inputTile = None
                if len(tiles[idx]["information"]["inputTileNames"]) > 0:
                    inputTile = tiles[idx]["information"]["inputTileNames"]
                information = {
                    "name":name,
                    "inputTile":inputTile
                }
                with open("info.pickle","wb") as f:
                    pickle.dump(information,f)
                # with open("code.py","w") as f:
                #     print("**************************!!!!!!!!!!!!!************")
                #     print(tiles[idx]["information"]["code"])
                #     f.write(tiles[idx]["information"]["code"])
                #     f.flush()
                f = open("code.py","w")
                print("()()()")
                print(tiles[idx]["information"]["code"])
                f.write(tiles[idx]["information"]["code"])
                f.flush()
                f.close()
                f = open("code.py","r")
                print("READING FROM CODE.PY AFTER WRITING")
                print(f.read())
                f.close()
                print("()()()")
                os.chdir("..")
    os.chdir(home)

def replicateToFileSystem(notebook,user="testpilot"):
    replicate(notebook)
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
    result = {"message":"no code output"}
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
                code_path = f"notebooks/{notebookName}/{tileName}/code.py"
                code = ""
                with open(code_path,"rb") as f:
                    code = f.read()
                env = {}
                obj = compile(code,"","exec")
                exec(obj,env)
                main = env["main"]
                result = main()
                main = None
            except Exception as e:
                errorString = str(e)    
                return jsonify({"message":errorString})
            if type(result)!=type(pd.DataFrame()):
                return jsonify({
                    "message":f"PyLot : [Error in main] Invalid function output type, expected Pandas dataframe got {type(result)}."
                })
            else:
                os.chdir(fileSystemValid)
                result.to_csv("output/output.csv")
                return jsonify(result.head(20).to_dict())
        else:
            #input type : Tile
                input_dataframes={}
                print(notebookName)
                for tile in information["inputTile"]:
                    #reading all the input dataframes into memory
                    path = f"notebooks/{notebookName}/{tile}/output"
                    try:
                        os.chdir(path)
                    except Exception as e:
                        exceptionString = str(e)
                        return jsonify({"error":exceptionString})
                    if "output.csv" not in os.listdir():
                        return jsonify({"error":f"{tile}'s output is missing. Could not execute {tileName}."})
                    else:
                        try:
                            '''
                            Finish off this block of code, dude.
                            '''
                            oo=pd.read_csv("output.csv")
                            if tile not in input_dataframes.keys():
                                input_dataframes[tile] = oo
                            else:
                                return jsonify({"error":"Duplicate input tile."})
                            os.chdir(HOME_PATH) #reset path after moving to current tile's folder.
                        except Exception as e:
                            return jsonify({"error":str(e)})
            #done reading dataframes to memory, now we select the dataframes that are actually used in the current tile's main function.
            #import the tile main as module [USE *args_array to unpack args into function arguments]
                os.chdir(HOME_PATH)#return to root.
                try:
                    #THE PROBLEM IS HERE
                    #If this file reading shit works better write some exception handlign for this shit.
                    code_path = f"notebooks/{notebookName}/{tileName}/code.py"
                    code = ""
                    with open(code_path,"rb") as f:
                        code = f.read()
                    print("READING CODE FROM FILE BEFORE EXEC")
                    print(code)
                    env = {}
                    obj = compile(code,"","exec")
                    exec(obj,env)
                    main = env["main"]
                    tile_main_args = getfullargspec(main).args
                except Exception as e:
                    # return jsonify({"error":str(e)+"\n Did you save the notebook?"})
                    return jsonify({"error":"error in file read code exec."})    
                if len(tile_main_args)>0:
                    #function has arguments
                    tile_input_dataframes = []
                    for arg in tile_main_args:
                        try:
                            tile_input_dataframes.append(input_dataframes[arg])
                        except KeyError:
                            return jsonify({"error":"Main function's formal arguments must have same name as input tiles to prevent ambiguity."})
                    # print(tile_input_dataframes) #remove this
                    #code execution
                    try:
                        result = main(*tile_input_dataframes)
                        os.chdir(f"notebooks/{notebookName}/{tileName}/output")
                        result.to_csv("output.csv")
                        main = None
                        return jsonify(result.head(20).to_dict())
                    except Exception as e:
                        return jsonify({"error":"[Error in tile code]: "+str(e)})
                else:
                    return jsonify({"error":"Tile input provided as one or more tiles but none used in main function."}) # maybe remove this case.
                return jsonify({"args":tile_main_args})
        os.chdir(HOME_PATH)        
        return jsonify({"message":"this is the last return, you shouldn't be seeing this."})     
    else:
        return jsonify({"message":"Notebook/Tile not found. (Save the notebook again)"})


if __name__=='__main__': 
    app.run(port = PORT, debug=True)
