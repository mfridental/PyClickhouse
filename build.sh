#!/usr/bin/env bash
rm dist/*
python setyp.py sdist
sudo /opt/anaconda/bin/pip install --upgrade dist/*

