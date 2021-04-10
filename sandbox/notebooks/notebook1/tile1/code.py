#This is example code written in a format expected from user.
import pandas as pd

def main():
    #the main function is what's run by the code execution engine.
    #always returns a pandas dataframe.
    oo = pd.read_csv("datasets/iris.csv") #whenever using datasets add the "datasets/" prefix
    result = oo.head()
    return result