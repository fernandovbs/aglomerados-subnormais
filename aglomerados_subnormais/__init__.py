import os
from flask import Flask
import click
from flask_pymongo import PyMongo
from flask_cors import CORS

app = Flask(__name__)
CORS(app)
app.config.from_object('aglomerados_subnormais.default_settings')
app.config.from_envvar('AGLOMERADOS_SUBNORMAIS_SETTINGS')
app.config['MONGO_DBNAME'] = 'aglomerados_subnormais'
app.config['MONGO_URI'] = 'mongodb://localhost:27017/aglomerados_subnormais'

mongo = PyMongo(app)

if not app.debug:
    import logging
    from logging.handlers import TimedRotatingFileHandler
    # https://docs.python.org/3.6/library/logging.handlers.html#timedrotatingfilehandler
    file_handler = TimedRotatingFileHandler(os.path.join(app.config['LOG_DIR'], 'aglomerados_subnormais.log'), 'midnight')
    file_handler.setLevel(logging.WARNING)
    file_handler.setFormatter(logging.Formatter('<%(asctime)s> <%(levelname)s> %(message)s'))
    app.logger.addHandler(file_handler)

@app.cli.command()
def load_dataset():
    click.echo('Iniciando processamento...')
    from . import dataset_handler
    dataset_handler.init_db()

import aglomerados_subnormais.views
