#!/usr/bin/env python3
"""Reject invalid release tags and mismatched artifact versions before publishing."""
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def validate(tag, version):
    if not re.fullmatch(r'v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)', tag):
        raise ValueError('Only stable release tags vMAJOR.MINOR.PATCH may publish images')
    if tag[1:] != version:
        raise ValueError(f'Tag {tag} does not match pom.xml version {version}')


if __name__ == '__main__':
    tag = sys.argv[1] if len(sys.argv) > 1 else ''
    if tag:
        root = ET.parse(Path(__file__).resolve().parents[1] / 'pom.xml').getroot()
        version = root.find('{http://maven.apache.org/POM/4.0.0}version').text
        validate(tag, version)
        print('Validated release ' + tag)
    else:
        print('Development build: no release tag to validate')
