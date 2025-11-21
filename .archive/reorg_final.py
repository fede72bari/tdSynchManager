import re

inp = r'src	dSynchManager\manager.py'
out = r'src	dSynchManager\manager_reorganized.py'

with open(inp, 'r', encoding='utf-8') as f:
    lines = f.readlines()
