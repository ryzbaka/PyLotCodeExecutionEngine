import os

def replicateToFileSystem(notebook,user):
    '''
    write code here to convert an object into a folder structure
    1) if there is not data folder create one
    2) if there if no notebooks folder create one
    3) Create a folder within the notebook folder named the same as the notebook object received.
    4) For each  of the tiles in the notebook object you'll have to create folder which contains a code.py file.
    5) The function that that code execution engine will target is the main function.
    '''
    # print("*****2")
    # print(notebook.keys())
    # print("*****3")
    # print(user)
    return f"***************\nFunctionality for creating {user}'s notebook {notebook['name']} is under development.\n***************" 