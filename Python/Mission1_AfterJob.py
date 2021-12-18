f=open("Misson1_Output/part-r-00000",'r')
lines=f.readlines()
f_to_write=open("Mission1_Real_Output",'w')
for line in lines:
    key=line.split("\t")[1].strip()
    value=line.split("\t")[0]
    f_to_write.write("%s %s\n"%(key,value))
