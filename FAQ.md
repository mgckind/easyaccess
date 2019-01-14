## Frequently Asked Questions

Here we collect a list of FAQ related to installation, usage, etc. Please refre to the [issues page](https://github.com/mgckind/easyaccess/issues) for more information.

- The installation with `pip` went smoothly but it was not obvious where "easyaccess" was installed.
    - When installed using pip, one can use the following: `pip show -f easyaccess` to check the installation folders

- When trying to connect I keep getting this error `ORA-21561: OID generation failed`. Any idea how to solve it?
    - Most of the time this problem can be solved by adding the name of your computer in the `/etc/hosts` file, next to the line that says 127.0.0.1 localhost. Just add the name of your computer (type `hostname`) to that line, so it looks like `127.0.0.1 localhost my-computer`
- Where is the configuration file and authentication file?
    - Usually the configuration file by default is at `$HOME/.easyaccess/config.ini`  and the authentication file is at `$HOME/.desservices.ini` or can be set at the env variable `$DES_SERVICES`
- I am a DES Collaborator, where can I reset my credentials?
  - For collaborators only: Please use [this](https://deslogin.wufoo.com/forms/help-me-with-my-desdm-account/) form
- The client hangs after getting an Oracle error and I need to close to the window.
  - This is a long-standing [issue](https://github.com/mgckind/easyaccess/issues/130) but we haven't been able to fix it or to reproduce it in all the systems. Please report it there and add details about OS and versions. We'd recommend a clean conda installation which sometimes fixes the issue.
- How can I install the Oracle client by myself.
  - Please follow the instructions posted [here](https://www.oracle.com/technetwork/database/database-technologies/instant-client/overview/index.html)
- Are there other requirements besides python modules and Oracle clients?
  - Usually not, for new OS or Virtual Machines you'd need to install `libaio` and `libbz2`
- How can I contribute to the project?
  - Please take a look st our [Code of Conduct](CODE_OF_CONDUCT.md) and or [contribution guide](CONTRIBUTING.md)
  
