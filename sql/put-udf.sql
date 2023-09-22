use WHYLOG_DEMO;

-- This can't be run from vscode for some reason
put 
file://./sleepy.py 
@funcs/ 
auto_compress=false
overwrite=true
;
