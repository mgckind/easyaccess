from easyaccess.eautils.ea_utils import *
import os
import stat

try:
    from builtins import input, str, range
except ImportError:
    from __builtin__ import input, str, range


try:
    import readline
    readline_present = True
    try:
        import gnureadline as readline
    except ImportError:
        pass
except ImportError:
    readline_present = False

class DatabaseActions(object):
    def do_execproc(self, line):
        """
        DB:Execute procedures in the DB, arguments can be floating numbers or strings

        Usage:
            execproc PROCEDURE('arg1', 'arg2', 10, 20, 'arg5', etc...)

        To see list of positional arguments and their data types use:
            execproc PROCEDURE() describe
        """
        if line == '':
            return self.do_help('execproc')
        line = line.replace(';', '')
        line = "".join(line.split())
        proc_name = line[0:line.index('(')]
        argument_list = line[line.index('(') + 1:line.index(')')].split(',')
        arguments = []
        for arg in argument_list:
            if arg.startswith(("\"", "\'")):
                arg = arg[1:-1]
            else:
                try:
                    arg = float(arg)
                except ValueError:
                    pass
            arguments.append(arg)
        if line[line.index(')'):].find('describe') > -1:
            comm = """\n Description of procedure '%s' """ % proc_name
            query = """
                select argument_name, data_type,position,in_out from all_arguments where
                 object_name = '%s' order by position""" % proc_name.upper()
            self.query_and_print(query, print_time=False, clear=True,
                                 extra=comm, err_arg="Procedure does not exist")
            return
        try:
            outproc = self.cur.callproc(proc_name, arguments)
            print(colored('Done!', "green", self.ct))
        except:
            print_exception(mode=self.ct)


    def do_describe_table(self, arg, clear=True, extra=None, return_df=False):
        """
        DB:This tool is useful for noting the lack of documentation
        for columns. If you don't know the full table name you can
        use tab completion on the table name. Tables of usual interest
        are described.

        Usage: describe_table <table_name>
        Describes the columns in <table-name> as
          column_name, oracle_Type, date_length, comments

        Optional: describe_table <table_name> with <pattern>
        Describes only the columns with certain pattern, example:

        describe_table Y1A1_COADD_OBJECTS with MAG%
        will describe only columns starting with MAG in that table
        """
        if arg == '':
            return self.do_help('describe_table')
        arg = arg.replace(';', '')
        arg = " ".join(arg.split())
        tablename = arg.split()[0]
        tablename = tablename.upper()
        pattern = None
        try:
            extra = arg.split()[1]
            if extra.upper() == 'WITH':
                pattern = arg.split()[2].upper()
        except:
            pass

        try:
            schema, table, link = self.get_tablename_tuple(tablename)
            # schema, table and link are now valid.
            link = "@" + link if link else ""
            qcom = """
            select comments from all_tab_comments%s atc
            where atc.table_name = '%s'""" % (link, table)
            comment_table = self.query_results(qcom)[0][0]
        except:
            try:
                schema, table, link = self.get_tablename_tuple(tablename)
                # schema, table and link are now valid.
                link = "@" + link if link else ""
                qcom = """
                select comments from all_mview_comments%s atc
                where atc.mview_name = '%s'""" % (link, table)
                comment_table = self.query_results(qcom)[0][0]
            except:
                comment_table = ''
                print(colored("Table not found.", "red", self.ct))
                return

        try:
            qnum = """
            select to_char(num_rows) from all_tables where
            table_name='%s' and  owner = '%s' """ % (table, schema)
            numrows_table = self.query_results(qnum)[0][0]
            if numrows_table is None:
                numrows_table = 'Not available'
        except:
            numrows_table = "Not available"

        # String formatting parameters
        params = dict(schema=schema, table=table, link=link,
                      pattern=pattern, comment=comment_table)

        if pattern:
            comm = """Description of %(table)s with """
            comm += """pattern %(pattern)s commented as: '%(comment)s'""" % params
            comm += "\nEstimated number of rows:" + colored(" %s" % numrows_table, "green", self.ct)
            q = """
            select atc.column_name, atc.data_type,
            case atc.data_type
            when 'NUMBER' then '(' || atc.data_precision || ',' || atc.data_scale || ')'
            when 'VARCHAR2' then atc.CHAR_LENGTH || ' characters'
            else atc.data_length || ''  end as DATA_FORMAT,
            acc.comments
            from all_tab_cols%(link)s atc , all_col_comments%(link)s acc
            where atc.owner = '%(schema)s' and atc.table_name = '%(table)s'
            and acc.owner = '%(schema)s' and acc.table_name = '%(table)s'
            and acc.column_name = atc.column_name
            and atc.column_name like '%(pattern)s'
            order by atc.column_name
            """ % params
        else:
            comm = """Description of %(table)s commented as: '%(comment)s'""" % params
            comm += "\nEstimated number of rows:" + colored(" %s" % numrows_table, "green", self.ct)
            q = """
            select atc.column_name, atc.data_type,
            case atc.data_type
            when 'NUMBER' then '(' || atc.data_precision || ',' || atc.data_scale || ')'
            when 'VARCHAR2' then atc.CHAR_LENGTH || ' characters'
            else atc.data_length || ''  end as DATA_FORMAT,
            acc.comments
            from all_tab_cols%(link)s atc , all_col_comments%(link)s acc
            where atc.owner = '%(schema)s' and atc.table_name = '%(table)s'
            and acc.owner = '%(schema)s' and acc.table_name = '%(table)s'
            and acc.column_name = atc.column_name
            order by atc.column_name
            """ % params

        if extra is None:
            extra = comm
        err_msg = 'Table does not exist or it is not accessible by user or pattern do not match'
        df = self.query_and_print(q, print_time=False,
                                  err_arg=err_msg, extra=extra, clear=clear, return_df=return_df)
        if return_df:
            return df
        return

    def complete_describe_table(self, text, line, start_index, end_index):
        return self._complete_tables(text)

    def do_find_tables(self, arg, extra=None, return_df=False):
        """
        DB:Lists tables and views matching an oracle pattern  e.g %SVA%,

        Usage : find_tables PATTERN
        """
        if extra is None:
            extra = 'To select from a table use owner.table_name' \
                    ' except for DESADMIN where only table_name is enough'
        if arg == '':
            return self.do_help('find_tables')
        arg = arg.replace(';', '')
        query = "SELECT owner,table_name from all_tables  WHERE upper(table_name) LIKE '%s' " % (
            arg.upper())
        df = self.query_and_print(query, extra=extra, return_df=return_df)
        if return_df:
            return df

    def complete_find_tables(self, text, line, start_index, end_index):
        return self._complete_tables(text)


    def do_load_table(self, line, name=None, chunksize=None, memsize=None):
        """
        DB:Loads a table from a file (csv or fits) taking name from filename and columns from header

        Usage: load_table <filename> [--tablename NAME] [--chunksize CHUNK] [--memsize MEMCHUNK]
        Ex: example.csv has the following content
             RA,DEC,MAG
             1.23,0.13,23
             0.13,0.01,22

        This command will create a table named EXAMPLE with 3 columns RA,DEC and MAG and
        values taken from file

        Optional Arguments:

            --tablename NAME            given name for the table, default is taken from filename
            --chunksize CHUNK           Number of rows to be inserted at a time.
                                        Useful for large files that do not fit in memory
            --memsize MEMCHUNK          The size in Mb to be read in chunks. If both specified,
                                        the lower number of rows is selected
                                        (the lower memory limitations)

        Note: - For csv or tab files, first line must have the column names (without # or any
        other comment) and same format as data (using ',' or space)
              - For fits file header must have columns names and data types
              - For filenames use <table_name>.csv or <table_name>.fits do not use extra points
        """
        line = line.replace(';', '')
        load_parser = KeyParser(prog='', usage='', add_help=False)
        load_parser.add_argument(
            'filename', help='name for the file', action='store', default=None)
        load_parser.add_argument(
            '--tablename', help='name for the table', action='store', default=None)
        load_parser.add_argument('--chunksize',
                                 help='number of rows to read in blocks to avoid memory issues',
                                 action='store', type=int, default=None)
        load_parser.add_argument('--memsize', help='size of the chunks to be read in Mb ',
                                 action='store', type=int, default=None)
        load_parser.add_argument(
            '-h', '--help', help='print help', action='store_true')
        try:
            load_args = load_parser.parse_args(line.split())
        except SystemExit:
            self.do_help('load_table')
            return
        if load_args.help:
            self.do_help('load_table')
            return
        filename = eafile.get_filename(load_args.filename)
        table = load_args.tablename
        invalid_chars = ['-', '$', '~', '@', '*']
        for obj in [table, name]:
            if obj is None:
                if any((char in invalid_chars) for char in filename):
                    print()
                    print(colored(
                        'Invalid table name, change filename or use --tablename\n', 'red', self.ct))
                    return
            else:
                if any((char in invalid_chars) for char in obj):
                    print(colored('\nInvalid table name\n', 'red', self.ct))
                    return

        chunk = load_args.chunksize
        memchunk = load_args.memsize
        if chunksize is not None:
            chunk = chunksize
        if memsize is not None:
            memchunk = memsize
        if memchunk is not None:
            memchunk_rows = eafile.get_chunksize(filename, memory=memchunk)
            if chunk is not None:
                chunk = min(chunk, memchunk_rows)
            else:
                chunk = memchunk_rows
        if filename is None:
            return
        base, ext = os.path.splitext(os.path.basename(filename))

        if ext == '.h5' and chunk is not None:
            print(colored("\nHDF5 file upload with chunksize is not supported yet. Try without "
                          "--chunksize\n", "red", self.ct))
            return

        if table is None:
            table = base
            if name is not None:
                table = name

        # check table first
        if self.check_table_exists(table):
            print(
                colored('\n Table already exists. Table can be removed with:', 'red', self.ct))
            print(colored(' DESDB ~> DROP TABLE %s;\n' %
                          table.upper(), 'red', self.ct))
            return

        try:
            data, iterator = eafile.read_file(filename)
        except:
            print_exception(mode=self.ct)
            return

        # Get the data in a way that Oracle understands

        iteration = 0
        done = False
        total_rows = 0
        if data.file_type == 'pandas':
            while not done:
                try:
                    if iterator:
                        df = data.get_chunk(chunk)
                    else:
                        df = data
                    df.file_type = 'pandas'
                    if len(df) == 0:
                        break
                    if iteration == 0:
                        dtypes = eafile.get_dtypes(df)
                    columns = df.columns.values.tolist()
                    values = df.values.tolist()
                    total_rows += len(df)
                except:
                    break
                if iteration == 0:
                    try:
                        self.create_table(table, columns, dtypes)
                    except:
                        print_exception(mode=self.ct)
                        self.drop_table(table)
                        return
                try:
                    if not done:
                        self.insert_data(
                            table, columns, values, dtypes, iteration)
                        iteration += 1
                        if not iterator:
                            done = True
                except:
                    print_exception(mode=self.ct)
                    self.drop_table(table)
                    return

        if data.file_type == 'fits':
            if chunk is None:
                chunk = data[1].get_nrows()
            start = 0
            while not done:
                try:
                    df = data
                    if iteration == 0:
                        dtypes = eafile.get_dtypes(df)
                        columns = df[1].get_colnames()
                    values = df[1][start:start + chunk].tolist()
                    start += chunk
                    if len(values) == 0:
                        break
                    total_rows += len(values)
                except:
                    break
                if iteration == 0:
                    try:
                        self.create_table(table, columns, dtypes)
                    except:
                        print_exception(mode=self.ct)
                        self.drop_table(table)
                        return

                try:
                    if not done:
                        self.insert_data(
                            table, columns, values, dtypes, iteration)
                        iteration += 1
                except:
                    print_exception(mode=self.ct)
                    self.drop_table(table)
                    return

        print(colored(
            '\n ** Table %s loaded successfully '
            'with %d rows.\n' % (table.upper(), total_rows), "green", self.ct))
        print(colored(
            ' You may want to refresh the metadata so your new '
            'table appears during\n autocompletion', "cyan", self.ct))
        print(colored(' DESDB ~> refresh_metadata_cache;', "cyan", self.ct))

        print()
        print(colored(' To make this table public run:', "blue", self.ct))
        print(colored(' DESDB ~> grant select on %s to DES_READER;' %
                      table.upper(), "blue", self.ct), '\n')
        return

    def complete_load_table(self, text, line, start_idx, end_idx):
        return complete_path(line)

    def do_append_table(self, line, name=None, chunksize=None, memsize=None):
        """
        DB:Appends a table from a file (csv or fits) taking its name from filename
        and the columns from header.

        Usage: append_table <filename> [--tablename NAME] [--chunksize CHUNK] [--memsize MEMCHUNK]
        Ex: example.csv has the following content
             RA,DEC,MAG
             1.23,0.13,23
             0.13,0.01,22

        This command will append the contents of example.csv to the table named EXAMPLE.
        It is meant to use after load_table command

         Optional Arguments:

              --tablename NAME           given name for the table, default is taken from filename
              --chunksize CHUNK          Number of rows to be inserted at a time. Useful for large
                                         files that do not fit in memory
              --memsize MEMCHUNK         The size in Mb to be read in chunks. If both specified,
                                         the lower number of rows is selected
                                         (the lower memory limitations)

        Note: - For csv or tab files, first line must have the column names
        (without # or any other comment) and same format as data (using ',' or space)
              - For fits file header must have columns names and data types
              - For filenames use <table_name>.csv or <table_name>.fits do not use extra points
        """
        line = line.replace(';', '')
        append_parser = KeyParser(prog='', usage='', add_help=False)
        append_parser.add_argument(
            'filename', help='name for the file', action='store', default=None)
        append_parser.add_argument(
            '--tablename', help='name for the table to append to', action='store', default=None)
        append_parser.add_argument('--chunksize',
                                   help='number of rows to read in blocks to avoid memory '
                                        'issues', action='store', default=None, type=int)
        append_parser.add_argument('--memsize', help='size of the chunks to be read in Mb ',
                                   action='store', type=int, default=None)
        append_parser.add_argument(
            '-h', '--help', help='print help', action='store_true')
        try:
            append_args = append_parser.parse_args(line.split())
        except SystemExit:
            self.do_help('append_table')
            return
        if append_args.help:
            self.do_help('append_table')
            return
        filename = eafile.get_filename(append_args.filename)
        table = append_args.tablename
        invalid_chars = ['-', '$', '~', '@', '*']
        for obj in [table, name]:
            if obj is None:
                if any((char in invalid_chars) for char in filename):
                    print(colored('\nInvalid table name, change filename '
                                  'or use --tablename\n', 'red', self.ct))
                    return
            else:
                if any((char in invalid_chars) for char in obj):
                    print(colored('\nInvalid table name\n', 'red', self.ct))
                    return

        chunk = append_args.chunksize
        memchunk = append_args.memsize
        if chunksize is not None:
            chunk = chunksize
        if memsize is not None:
            memchunk = memsize
        if memchunk is not None:
            memchunk_rows = eafile.get_chunksize(filename, memory=memchunk)
            if chunk is not None:
                chunk = min(chunk, memchunk_rows)
            else:
                chunk = memchunk_rows

        if filename is None:
            return
        base, ext = os.path.splitext(os.path.basename(filename))

        if ext == '.h5' and chunk is not None:
            print(colored("\nHDF5 file upload with chunksize is not supported yet. Try without "
                          "--chunksize\n", "red", self.ct))
            return

        if table is None:
            table = base
            if name is not None:
                table = name

        # check table first
        if not self.check_table_exists(table):
            print('\n Table does not exist. Table can be created with:'
                  '\n DESDB ~> CREATE TABLE %s '
                  '(COL1 TYPE1(SIZE), ..., COLN TYPEN(SIZE));\n' % table.upper())
            return
        try:
            data, iterator = eafile.read_file(filename)
        except:
            print_exception(mode=self.ct)
            return

        iteration = 0
        done = False
        total_rows = 0
        if data.file_type == 'pandas':
            while not done:
                try:
                    if iterator:
                        df = data.get_chunk(chunk)
                    else:
                        df = data
                    df.file_type = 'pandas'
                    if len(df) == 0:
                        break
                    if iteration == 0:
                        dtypes = eafile.get_dtypes(df)
                    columns = df.columns.values.tolist()
                    values = df.values.tolist()
                    total_rows += len(df)
                except:
                    break
                try:
                    if not done:
                        self.insert_data(
                            table, columns, values, dtypes, iteration)
                        iteration += 1
                        if not iterator:
                            done = True
                except:
                    print_exception(mode=self.ct)
                    return

        if data.file_type == 'fits':
            if chunk is None:
                chunk = data[1].get_nrows()
            start = 0
            while not done:
                try:
                    df = data
                    if iteration == 0:
                        dtypes = eafile.get_dtypes(df)
                        columns = df[1].get_colnames()
                    values = df[1][start:start + chunk].tolist()
                    start += chunk
                    if len(values) == 0:
                        break
                    total_rows += len(values)
                except:
                    break
                try:
                    if not done:
                        self.insert_data(
                            table, columns, values, dtypes, iteration)
                        iteration += 1
                except:
                    print_exception(mode=self.ct)
                    return

        print(colored('\n ** Table %s appended '
                      'successfully with %d rows.' % (table.upper(), total_rows), "green", self.ct))


    def complete_append_table(self, text, line, start_idx, end_idx):
        return complete_path(line)


    def do_add_comment(self, line):
            """
            DB:Add comments to table and/or columns inside tables

            Usage:
                - add_comment table <TABLE> 'Comments on table'
                - add_comment column <TABLE.COLUMN> 'Comments on columns'

            Ex:  add_comment table MY_TABLE 'This table contains info'
                 add_comment columns MY_TABLE.ID 'Id for my objects'

            This command supports smart-autocompletion. No `;` is
            necessary (and it will be inserted into comment if used).

            """

            line = " ".join(line.split())
            keys = line.split()
            oneline = "".join(keys)
            if oneline.find('table') > -1:
                if len(keys) == 1:
                    print(colored('\nMissing table name\n', "red", self.ct))
                    return
                table = keys[1]
                if len(keys) == 2:
                    print(colored('\nMissing comment for table %s\n' %
                                  table, "red", self.ct))
                    return
                comm = " ".join(keys[2:])
                if comm[0] == "'":
                    comm = comm[1:-1]
                qcom = """comment on table %s is '%s'""" % (table, comm)
                message = "Comment added to table: %s" % table
                self.query_and_print(qcom, print_time=False, suc_arg=message)
            elif oneline.find('column') > -1:
                if len(keys) == 1:
                    print(colored('\nMissing column name (TABLE.COLUMN)\n', "red", self.ct))
                    return
                col = keys[1]
                if len(keys) == 2:
                    print(colored('\nMissing comment for column %s\n' %
                                  col, "red", self.ct))
                    return
                if len(keys) > 2 and col.find('.') == -1:
                    print(colored('\nMissing column name for table %s\n',
                                  "red", self.ct) % col)
                    return
                comm = " ".join(keys[2:])
                if comm[0] == "'":
                    comm = comm[1:-1]
                qcom = """comment on column  %s is '%s'""" % (col, comm)
                message = "Comment added to column: %s in table %s" % (
                    col.split('.')[1], col.split('.')[0])
                self.query_and_print(qcom, print_time=False, suc_arg=message)
            else:
                print(colored('\nMissing arguments\n', "red", self.ct))
                self.do_help('add_comment')

    def complete_add_comment(self, text, line, begidx, lastidx):
        if line:
            oneline = "".join(line.strip())
            if oneline.find('table') > -1:
                return self._complete_tables(text)
            elif oneline.find('column') > -1:
                if oneline.find('.') > -1:
                    colname = text.split('.')[-1]
                    tablename = text.split('.')[0]
                    return [tablename + '.' + cn for cn in
                            self._complete_colnames(colname) if cn.startswith(colname)]
                else:
                    return self._complete_tables(text)
            else:
                return [option for option in options_add_comment if option.startswith(text)]
        else:
            return options_add_comment


    def do_edit(self, line):
        """
        Opens a buffer file to edit a sql statement and then it reads it
        and executes the statement. By default it will show the current
        statement in buffer (or empty)

        Usage:
            - edit   : opens the editor (default from $EDITOR or nano)
            - edit set_editor <editor> : sets editor to <editor>, ex: edit set_editor vi
        """

        line = "".join(line.split())
        if line.find('show') > -1:
            print('\nEditor  = {:}\n'.format(self.editor))
        elif line.find('set_editor') > -1:
            val = line.split('set_editor')[-1]
            if val != '':
                self.editor = val
                self.config.set('easyaccess', 'editor', val)
                self.writeconfig = True
        else:
            os.system(self.editor + ' easy.buf')
            if os.path.exists('easy.buf'):
                newquery = read_buf('easy.buf')
                if newquery == "":
                    return
                print()
                print(newquery)
                print()
                if (input('submit query? (Y/N): ') in ['Y', 'y', 'yes']):
                    self.default(newquery)

    def complete_edit(self, text, line, start_index, end_index):
        if text:
            return [option for option in options_edit if option.startswith(text)]
        else:
            return options_edit


    def do_loadsql(self, line):
        """
        DB:Loads a sql file with a query and ask whether it should be run
        There is a shortcut using @, ex : @test.sql  (or @test.sql > myfile.csv
        to override output file)

        Usage: loadsql <filename with sql statement>   (use autocompletion)

        Optional: loadsql <filename with sql statement> > <output_file> to
        write to a file, not to the screen
        """
        line = line.replace(';', '')
        if line.find('>') > -1:
            try:
                line = "".join(line.split())
                newq = read_buf(line.split('>')[0])
                if newq.find(';') > -1:
                    newq = newq.split(';')[0]
                outputfile = line.split('>')[1]
                newq = newq + '; > ' + outputfile
            except:
                outputfile = ''

        else:
            newq = read_buf(line)

        if newq == "":
            return
        if self.interactive:
            print()
            print(newq)
            print()
            if (input('submit query? (Y/N): ') in ['Y', 'y', 'yes']):
                self.default(newq)
        else:
            self.default(newq)

    def complete_loadsql(self, text, line, start_idx, end_idx):
        return complete_path(line)


    def do_refresh_metadata_cache(self, arg):
        """
        DB:Refreshes meta data cache for auto-completion of table
        names and column names .
        """
        verb = True
        try:
            if verb:
                print('Loading metadata into cache...')
            self.cache_table_names = self.get_tables_names()
            self.cache_usernames = self.get_userlist()
            self.cache_column_names = self.get_columnlist()
        except:
            if verb:
                print(
                    colored("There was an error when refreshing the metadata", "red", self.ct))
        try:
            self.cur.execute('create table FGOTTENMETADATA (ID int)')
        except:
            pass


    def do_show_db(self, arg):
        """
        DB:Shows database connection information
        """
        lines = "user: %s\ndb  : %s\nhost: %s\n" % (
            self.user.upper(), self.dbname.upper(), self.dbhost.upper())
        lines = lines + "\nPersonal links:"
        query = """
        SELECT owner, db_link, username, host, created
        FROM all_db_links where OWNER = '%s'
        """ % (self.user.upper())
        self.query_and_print(query, print_time=False, extra=lines, clear=True)


    def do_myquota(self, arg):
        """
        DB:Print information about quota status.

        Usage: myquota
        """
        query = """
        SELECT tablespace_name, mbytes_used/1024 as GBYTES_USED,
        mbytes_left/1024 as GBYTES_LEFT from myquota
        """
        self.query_and_print(query, print_time=False, clear=True)

    def do_mytables(self, arg, return_df=False, extra="List of my tables"):
        """
        DB:Lists tables you have made in your user schema.

        Usage: mytables
        """
        # query = "SELECT table_name FROM user_tables"
        query = """
        SELECT t.table_name, s.bytes/1024/1024/1024 SIZE_GBYTES
        FROM user_segments s, user_tables t
        WHERE s.segment_name = t.table_name order by t.table_name
        """

        df = self.query_and_print(
            query, print_time=False, extra=extra, clear=True, return_df=return_df)
        if return_df:
            return df

    def do_user_tables(self, arg):
        """
        DB:List tables from given user

        Usage: user_tables <username>
        """
        if arg == "":
            return self.do_help('user_tables')
        return self.get_tables_names_user(arg)

    def do_show_index(self, arg):
        """
        DB:Describes the indices  in <table-name> as
          column_name, oracle_Type, date_length, comments

         Usage: describe_index <table_name>
        """

        if arg == '':
            return self.do_help('show_index')
        arg = arg.replace(';', '')
        arg = " ".join(arg.split())
        tablename = arg.split()[0]
        tablename = tablename.upper()

        try:
            schema, table, link = self.get_tablename_tuple(tablename)
            link = "@" + link if link else ""
        except:
            print(colored("Table not found.", "red", self.ct))
            return

        # Parse tablename for simple name or owner.tablename.
        # If owner present, then add owner where clause.

        params = dict(schema=schema, table=table, link=link)
        query = """
        SELECT UNIQUE tab.table_name,icol.column_name,
        idx.index_type,idx.index_name
        FROM all_tables%(link)s tab
        JOIN all_indexes%(link)s idx on idx.table_name = tab.table_name
        JOIN all_ind_columns%(link)s icol on idx.index_name = icol.index_name
        WHERE tab.table_name='%(table)s' and tab.owner = '%(schema)s'
        ORDER BY icol.column_name,idx.index_name
        """ % params
        nresults = self.query_and_print(query)
        return

    def complete_show_index(self, text, line, begidx, lastidx):
        return self._complete_tables(text)


    def get_tablename_tuple(self, tablename):
        """
        Return the tuple (schema,table,link) that can be used to
        locate the fundamental definition of the table requested.
        """
        table = tablename
        schema = self.user.upper()  # default --- Mine
        link = ""  # default no link

        if "." in table:
            (schema, table) = table.split(".")
        if "@" in table:
            (table, link) = table.split("@")

        # Loop until we find a fundamental definition OR determine there is
        # no reachable fundamental definition, follow links and resolve
        # schema names. Rely on how the DES database is constructed we log
        # into our own schema, and rely on synonyms for a "simple" view of
        # common schema.

        while (1):
            # check for fundamental definition  e.g. schema.table@link
            q = """
            select count(*) from ALL_TAB_COLUMNS%s
            where OWNER = '%s' and TABLE_NAME = '%s'
            """ % ("@" + link if link else "", schema, table)
            ans = self.query_results(q)
            if ans[0][0] > 0:
                # found real definition return the tuple
                return (schema, table, link)

            # check if we are indirect by synonym of user
            q = """
            select TABLE_OWNER, TABLE_NAME, DB_LINK from USER_SYNONYMS%s
            where SYNONYM_NAME = '%s'
            """ % ("@" + link if link else "", table)
            ans = self.query_results(q)
            if len(ans) == 1:
                # resolved one step closer to fundamental definition
                (schema, table, link) = ans[0]
                continue

            # check if we are indirect by a public synonym
            q = """
            select TABLE_OWNER, TABLE_NAME, DB_LINK from ALL_SYNONYMS%s
            where SYNONYM_NAME = '%s' AND OWNER = 'PUBLIC'
            """ % ("@" + link if link else "", table)
            ans = self.query_results(q)
            if len(ans) == 1:
                # resolved one step closer to fundamental definition
                (schema, table, link) = ans[0]
                continue

            # failed to find the reference to the table
            # no such table accessible by user
            break

        msg = "No table found for: %s" % tablename
        raise Exception(msg)
