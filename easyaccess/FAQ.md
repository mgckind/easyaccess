## Frequently Asked Questions

Here we collect a list of FAQ related to installation, usage, etc. Please refre to the [issues page](https://github.com/mgckind/easyaccess/issues) for more information.

- The installation with `pip` went smoothly but it was not obvious where "easyaccess" was installed.
    - When installed using pip, one can use the following: `pip show -f easyaccess` to check the installation folders

- When trying to connect I keep getting this error `ORA-21561: OID generation failed`. Any idea how to solve it?
    - Most of the time this problem can be solved by adding the name of your computer in the `/etc/hosts` file, next to the line that says 127.0.0.1 localhost. Just add the name of your computer (type `hostname`) to that line, so it looks like `127.0.0.1 localhost my-computer`
