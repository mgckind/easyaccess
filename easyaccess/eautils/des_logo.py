from __future__ import print_function
__author__ = 'Matias Carrasco Kind'
import sys
try:
    from termcolor import colored as c
except ImportError:
    def c(line, color):
        return line


def noc(line, color):
    return line


def print_deslogo(color=True):
    char0 = u"\u203E"
    char1 = u"\u203E"
    char2 = u"\u00B4"
    if sys.stdout.encoding != 'UTF-8':
        char0 = ' '
        char1 = '-'
        char2 = '`'
    if color:
        c2 = c
    else:
        c2 = noc
    L = []
    if sys.stdout.encoding != 'UTF-8':
        L.append("     _______      ")
    L.append("""     \\"""+char0*6+"""\      """)
    L.append("  "+c2("//", "red")+" / .    .\    ")
    L.append(" "+c2("//", "red")+" /   .    _\   ")
    L.append(c2("//", "red")+" /  .     / "+c2("//", "red")+" ")
    L.append(c2("\\\\", "red")+" \     . / "+c2("//", "red")+"  ")
    L.append(c2(" \\\\", "red")+" \_____/ "+c2("//", "red")+"   ")
    L.append(c2("  \\\\_______//", "red")+"    DARK ENERGY SURVEY")
    last = c2("""   `"""+char1*7+char2, "red") + "     DATA MANAGEMENT"
    L.append(last)

    print()
    for l in L:
        print(l)


if __name__ == "__main__":
    print()
    print_deslogo()
