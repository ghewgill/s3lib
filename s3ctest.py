import os

if False:
    os.system("s3c create /")
    os.system("s3c create TEST.s3c")
    os.system("s3c create TEST.s3c/")
    os.system("s3c create /TEST.s3c")
os.system("s3c create /TEST.s3c/")

out = os.popen("s3c put TEST.s3c/test", "w")
print >>out, "moo"
out.close()

os.system("s3c list TEST.s3c")
os.system("s3c list TEST.s3c/")
os.system("s3c list /TEST.s3c")
os.system("s3c list /TEST.s3c/")
os.system("s3c list TEST.s3c TEST.s3c")

os.system("s3c ls TEST.s3c")
os.system("s3c ls TEST.s3c/")
os.system("s3c ls /TEST.s3c")
os.system("s3c ls /TEST.s3c/")
os.system("s3c ls TEST.s3c TEST.s3c")

os.system("s3c get TEST.s3c/test")
os.system("s3c get /TEST.s3c/test")

os.system("s3c delete TEST.s3c/test")
os.system("s3c delete TEST.s3c")
