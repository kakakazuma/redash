Users' Management
#################

If you use Google OpenID authentication, then each user from the domains
you allowed will automatically be logged in and have the default
permissions.

If you want to give some user different permissions or you want to
create password based users (make sure you enabled this options in
settings first), you need to use the CLI (``manage.py``).

Create a new user
=================

.. code:: bash

    $ bin/run ./manage.py users create --help
    usage: users create [-h] [--permissions PERMISSIONS] [--password PASSWORD]
                        [--google] [--admin]
                        name email

    positional arguments:
      name                  User's full name
      email                 User's email

    optional arguments:
      -h, --help            show this help message and exit
      --permissions PERMISSIONS
                            Comma seperated list of permissions (leave blank for
                            default).
      --password PASSWORD   Password for users who don't use Google Auth (leave
                            blank for prompt).
      --google              user uses Google Auth to login
      --admin               set user as admin

Grant admin permissions
=======================

``sudo -u redash bin/run ./manage.py users grant_admin {email}``
