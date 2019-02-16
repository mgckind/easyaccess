## Frequently Asked Questions

Here we collect a list of FAQ related to installation, usage, etc. Please refer to the [issues page](https://github.com/mgckind/easyaccess/issues) for more information.

- **Q: Installation with `pip` went smoothly, but where was `easyaccess` installed?**
  - A: When installed using pip, one can use the following: `pip show -f easyaccess` to check the installation folders.
- **Q: When I try to connect I get the error: `ORA-21561: OID generation failed`. How do I solve this?**
    - A: Most of the time this problem can be solved by adding the name of your computer in the `/etc/hosts` file, next to the line that says `127.0.0.1 localhost`. Add the name of your computer (i.e. the output of the `hostname` command) to that line, so it looks like `127.0.0.1 localhost <HOSTNAME>`.
- **Q: Where is the configuration file and authentication file?**
    - A: By default, the configuration file is located at `$HOME/.easyaccess/config.ini`  and the authentication file is at `$HOME/.desservices.ini` or can be set at the environment variable `$DES_SERVICES`.
- **Q: I am a DES Collaborator, where can I reset my credentials?**
  - A: For collaborators only: Please use [this](https://deslogin.wufoo.com/forms/help-me-with-my-desdm-account/) form.
- **Q: The client hangs after getting an Oracle error, and I need to close to the window. Can I fix this?**
  - A: This is a long-standing [issue](https://github.com/mgckind/easyaccess/issues/130) that we haven't been able to fix or reproduce on all the systems. Please report it on [issue #130](https://github.com/mgckind/easyaccess/issues/130) and add details about your OS and easyaccess version. We'd recommend a clean conda installation, which sometimes fixes the issue.
- **Q: How can I install the Oracle client myself?**
  - A: Please follow the instructions posted [here](https://www.oracle.com/technetwork/database/database-technologies/instant-client/overview/index.html).
- **Q: Are there other requirements besides python modules and Oracle clients?**
  - A: Usually not, for new OS or Virtual Machines you'd need to install `libaio` and `libbz2`.
- **Q: How can I contribute to the project?**
  - A: Contributions are welcome! Please take a look at our [Code of Conduct](CODE_OF_CONDUCT.md) and [Contribution guide](CONTRIBUTING.md).
