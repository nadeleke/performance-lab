#!/usr/bin/env bash
rsync -rav -e ssh --exclude='*.xml' --exclude='*local_settings.py' . webserver:/home/ubuntu/webserver