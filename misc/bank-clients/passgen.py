for i in range(1000):
    file1 = open("passwd.txt", "a")
    file1.write(str(7000+i)+"\n")
    file1.close()
