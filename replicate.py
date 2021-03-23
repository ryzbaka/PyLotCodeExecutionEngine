import os
import pickle
import hashlib
from shutil import rmtree
from picklehash import getOldHash

def getNewHash():
    fileObject = open("incomingNotebook.pickle", "rb")
    pickleBytes = fileObject.read()
    sha1 = hashlib.sha1(pickleBytes)
    hashed = sha1.hexdigest()
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
            with open("code.py","w") as codeObject:
                codeObject.write(tiles[idx]["information"]["code"])
            os.chdir("..")
    else:
        print("~MODIFYING EXISTING NOTEBOOK")
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
                with open("code.py","w") as codeObject:
                    codeObject.write(tiles[idx]["information"]["code"])
            else:
                os.chdir(name)
                with open("code.py","w") as codeObject:
                    codeObject.write(tiles[idx]["information"]["code"])
                os.chdir("..")
    ##############
    os.chdir(home)


def replicateToFileSystem(notebook,user="testpilot"):
    '''
    write code here to convert an object into a folder structure
    1) if there is not data folder create one
    2) if there if no notebooks folder create one
    3) Create a folder within the notebook folder named the same as the notebook object received.
    4) For each  of the tiles in the notebook object you'll have to create folder which contains a code.py file.
    5) The function that that code execution engine will target is the main function.
    '''
    username = user
    if "lastNotebook.pickle" not in os.listdir(): #change to <oldnotebookname>.pickle
        with open("lastNotebook.pickle","wb") as f:
            pickle.dump(notebook, f)
        #notebook being passed into the file system for the first time, no comparison needed.
    else:
        #new notebook object received, compare pickle hashes
        with open("incomingNotebook.pickle", "wb") as f:
            pickle.dump(notebook, f)
        oldHash = getOldHash() 
        newHash = getNewHash()
        if oldHash == newHash:
            print("same")
        else:
            with open("lastNotebook.pickle","wb") as f:
                pickle.dump(notebook,f)
            replicate(notebook)

    return f"***************\n***************" 