# Roller Balance Web Service

Crypto settlement service for roller game

A Flask application that runs and serves a RESTful API for the roller-balance server.

Settings are read from `roller.env` file if it exists. See `roller.env.example` for optional settings.

## Requirements

- Python 3.9 or above, including the venv module (`apt install python3-venv` on debian compatible distributions)

## Setup

To create and initialize the database, run the following command from a bash session to start a live python admin shell:
```sh
./deploy.sh shell
```

Note: during the first run of the deploy script, it will install a local virtual python environment with all required libraries. This happens only once.

In the shell that opens, type the following command:
```python
import db
db.nuke_database_and_create_new_please_think_twice()
```

Notes:
- this will drop any existing database named `roller`, or whatever name you set in `roller.env`.
- for the setup, you need to use user that has the required privileges to create new databases, and the same privileges are also required when running tests (which create a temporary database), but for regular running of the server, only SELECT and INSERT privileges over the created database are required.

To check that everything is installed properly, run the following command from a bash session:
```sh
./deploy.sh test
```

## Deployment

To start the server:
```sh
./deploy.sh run
```

To stop the server:
```sh
./deploy.sh kill-listener
```
