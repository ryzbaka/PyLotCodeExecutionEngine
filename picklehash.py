import os
import hashlib


def getOldHash():
    fileObject = open("lastNotebook.pickle", "rb")
    pickleBytes = fileObject.read()
    sha1 = hashlib.sha1(pickleBytes)
    hashed = sha1.hexdigest()
    return hashed