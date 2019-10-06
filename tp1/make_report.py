import os

files = [os.path.join(dirpath, fn) 
            for dirpath, _, files in os.walk(".") 
            for fn in files 
            if ".go" in fn and "swp" not in fn]

with open("code.md", "w") as out:
    out.write("---\ntoc: yes\ngeometry: margin=2cm\n---\n\n")
    for fn in files:
        out.write("## {} \n\n".format(fn))
        out.write("```go \n")
        with open(fn, "r") as codefile:
            line = codefile.readline()
            while line:
                out.write(line)
                line = codefile.readline()
        out.write("\n```\n\n")



