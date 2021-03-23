import pandas as pd

def main():
    oo = pd.read_csv("data/iris.csv")
    result = oo.head()
    return oo.head()
