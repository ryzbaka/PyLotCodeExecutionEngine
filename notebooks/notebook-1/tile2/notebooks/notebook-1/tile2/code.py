import pandas as pd

def main():
    oo = pd.read_csv("datasets/iris.csv")
    result = oo.tail()
    return result