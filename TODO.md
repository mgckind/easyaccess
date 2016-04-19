# To-do list

- run multiple queries from a sql file
- add exec like for procedures, add do_execute(self, procedure, args)
- deal with date time variables
- argparser argument groups
- pass name of upload table as optional parameter from command line/prompt
- add views to describe_table and find_table (only tables are listed)
- add functionality for grabbing images (SE or COADD) by object or ra,dec
- should describe_table sort by column name?
- revamp the color control (can't turn off in API)
- add real-time toggle for termcolor using @property decorator around a easy_or._color_terminal property.
- move config_ea.py to eautils/config.py
- parse config into easy_or object more automatically
- Only catch exceptions at top level (cmdloop); raise everywhere else.
- Add '--debug' command to print more output/raise exceptions