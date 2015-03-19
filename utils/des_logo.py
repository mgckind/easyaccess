__author__ = 'Matias Carrasco Kind'
try:
    from termcolor import colored as c
except:
    def c(line, color):
        return line

def noc(line,color):
    return line
def print_deslogo(color=True):
    if color:
        c2=c
    else:
        c2=noc
    L=[]
    #L.append("     ______      ")
    L.append("""     \\"""+u"\u203E"*6+"""\      """)
    L.append("  "+c2("//","red")+" / .    .\    ")
    L.append(" "+c2("//","red")+" /   .    _\   ")
    L.append(c2("//","red")+" /  .     / "+c2("//","red")+" ")
    L.append(c2("\\\\","red")+" \     . / "+c2("//","red")+"  ")
    L.append(c2(" \\\\","red")+" \_____/ "+c2("//","red")+"   ")
    L.append(c2("  \\\\_______//","red")+"    DARK ENERGY SURVEY")
    last=c2("""   `"""+u"\u203E"*7+u"\u00B4","red") +"     DATA MANAGEMENT"
    L.append(last)

    print 
    for l in L: print l

if __name__ == "__main__":
    print
    print_deslogo()
