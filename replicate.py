import os
import pickle
import hashlib
from shutil import rmtree

def getHash(pickleFile):
    fileObject = open(pickleFile,"rb")
    pickleBytes = fileObject.read()
    sha = hashlib.sha1(pickleBytes)
    hashed = sha.hexdigest()
    return hashed    
def fsInit():
    print("Initializing filesystem...")
    os.mkdir("notebooks")
    os.mkdir("data")

def replicate(notebook):
    # print(f"replicating notebook object to file system")
    notebookName = notebook["name"]
    tiles = notebook["tiles"]
    tileNames = notebook["tileNames"]
    home = os.path.abspath(os.curdir)
    if ("notebooks" not in os.listdir()) or ("data" not in os.listdir()):
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
            print(information)
            with open("info.pickle","wb") as f: #!!!!!!!
                pickle.dump(information,f)

            with open("code.py","w") as codeObject:
                codeObject.write(tiles[idx]["information"]["code"])
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
                with open("code.py","w") as codeObject:
                    codeObject.write(tiles[idx]["information"]["code"])
                os.chdir("..")
    os.chdir(home)


def replicateToFileSystem(notebook,user="testpilot"):
    username = user
    notebookName = notebook['name']
    lastPickle = f"last{notebookName}.pickle"
    incomingPickle = f"incoming{notebookName}.pickle"
    print(lastPickle)
    print(incomingPickle) 
    if lastPickle not in os.listdir(): #change to <oldnotebookname>.pickle
        with open(lastPickle,"wb") as f:
            pickle.dump(notebook, f)
        replicate(notebook)
        #notebook being passed into the file system for the first time, no comparison needed.
    else:
        #new notebook object received, compare pickle hashes
        with open(incomingPickle, "wb") as f:
            pickle.dump(notebook, f)
        oldHash = getHash(lastPickle) 
        newHash = getHash(incomingPickle)
        if oldHash == newHash:
            print("same")
        else:
            with open(lastPickle,"wb") as f:
                pickle.dump(notebook,f)
            replicate(notebook)

    return f"***************\n***************" 