class Do_Func(object): 
    def do_help(self, arg):
        """
        List available commands with "help" or detailed help with "help cmd".
        """
        if arg:
            # TODO check arg syntax
            try:
                func = getattr(self, 'help_' + arg)
            except AttributeError:
                try:
                    doc = getattr(self, 'do_' + arg).__doc__
                    if doc:
                        doc = str(doc)
                        if doc.find('DB:') > -1:
                            doc = doc.replace('DB:', '')
                        if arg in NOT_PUBLIC and self.dbname == 'desdr':
                            doc = colored('\n\t* Command not availble in Public Release DB *\n',
                                          'red', self.ct) + doc
                        self.stdout.write("%s\n" % str(doc))
                        return
                except AttributeError:
                    pass
                self.stdout.write("%s\n" % str(self.nohelp % (arg,)))
                return
            func()
        else:
            self.do_clear(True)
            dl.print_deslogo(self.ct)
            self.stdout.write(str(self.intro) + "\n")
            names = self.get_names()
            cmds_doc = []
            cmds_undoc = []
            cmds_db = []
            help = {}
            for name in names:
                if name[:5] == 'help_':
                    help[name[5:]] = 1
            names.sort()
            # There can be duplicates if routines overridden
            prevname = ''
            for name in names:
                if name[:3] == 'do_':
                    if name == prevname:
                        continue
                    prevname = name
                    cmd = name[3:]
                    if cmd in NOT_PUBLIC and self.dbname == 'desdr':
                        continue
                    if cmd in help:
                        cmds_doc.append(cmd)
                        del help[cmd]
                    elif getattr(self, name).__doc__:
                        doc = getattr(self, name).__doc__
                        if doc.find('DB:') > -1:
                            cmds_db.append(cmd)
                        else:
                            cmds_doc.append(cmd)
                    else:
                        cmds_undoc.append(cmd)
            self.stdout.write("%s\n" % str(self.doc_leader))
            self.print_topics(self.doc_header, cmds_doc, 80)
            self.print_topics(self.docdb_header, cmds_db, 80)
            self.print_topics(self.misc_header, list(help.keys()), 80)
            self.print_topics(self.undoc_header, cmds_undoc, 80)

            print(colored(' *Default Input*', 'cyan', self.ct))
            print(self.ruler * 80)
            print("* To run SQL queries just add ; at the end of query")
            print("* To write to a file  : select ... from ... "
                  "where ... ; > filename")
            print(colored(
                "* Supported file formats (.csv, .tab., .fits, .h5) ",
                "green", self.ct))
            print("* To check SQL syntax : select ... from ... "
                  "where ... ; < check")
            print(
                "* To see the Oracle execution plan  : select ... "
                "from ... where ... ; < explain")
            print()
            print("* To access an online tutorial type: online_tutorial ")
