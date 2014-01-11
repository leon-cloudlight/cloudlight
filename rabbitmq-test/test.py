#!/usr/bin/python

import json

dictVal = { "father": "leon",
            "daughters": [ 'cindy', 'laura']
          }
print json.dumps(dictVal)

jsonStr = '{"mather": "grace"}'
dictVal2 = json.loads(jsonStr)
print dictVal2[u"mather"]

import decimal
decimal.getcontext().prec = 2
print decimal.Decimal('0.243352')

