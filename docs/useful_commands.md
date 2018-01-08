Useful Commands
===============


General
-------

### Start collector
`bob -l DEBUG --project test collector -a pyh3 -a grp2`

### Import dump
`bob -l INFO --project test simulate --agent phy3 12288 61440  --agent grp2 4096 63488  ~/Sindabus/Datensammlungen/KNX\ Dump/eiblog.txt`

AddrAnalyser
------------

### train
`bob -l INFO --project test train addr --start "2012-02-27T00:00:00" --end "2012-03-05T00:00:00" -m tmp/addr_model.json`

### analyse
`bob -l INFO --project test analyse addr -m tmp/addr_model.json`
