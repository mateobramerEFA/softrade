#!/bin/bash
gunicorn --bind=0.0.0.0:8000 --timeout 600 --worker-class gevent --workers 2 app:app