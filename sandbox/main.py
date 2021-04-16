def main():
    path = "codes/code.py"
    env = {}
    code = ""
    with open(path,"rb") as f:
        code = f.read()
    obj = compile(code,"","exec")
    exec(obj,env)
    f = env["main"]
    f()

if __name__=='__main__':
    while True:
        userin = input("1 for code run >")
        if userin == "1":
            main()
        else:
            quit()
