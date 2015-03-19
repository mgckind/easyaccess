__author__ = 'Matias Carrasco Kind'
try:
    from termcolor import colored as c
except:
    def c(line, color):
        return line
def print_deslogo():
    L=[]
    #L.append("     ______      ")
    L.append("""     \\"""+u"\u203E"*6+"""\      """)
    L.append("  "+c("//","red")+" / .    .\    ")
    L.append(" "+c("//","red")+" /   .    _\   ")
    L.append(c("//","red")+" /  .     / "+c("//","red")+" ")
    L.append(c("\\\\","red")+" \     . / "+c("//","red")+"  ")
    L.append(c(" \\\\","red")+" \_____/ "+c("//","red")+"   ")
    L.append(c("  \\\\_______//","red")+"    DARK ENERGY SURVEY")
    last=c("""   `"""+u"\u203E"*7+u"\u00B4","red") +"     DATA MANAGEMENT"
    L.append(last)

    print 
    for l in L: print l

if __name__ == "__main__":
    print
    print_deslogo()
