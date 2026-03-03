#!/usr/bin/env python3
import sys
import json
import time
import os

def main():
    if len(sys.argv) < 3:
        return
        
    key = sys.argv[1]
    status = sys.argv[2]
    msg = sys.argv[3] if len(sys.argv) > 3 else None
    
    # Load redis config from job_config (assumed in CWD or parent?)
    # passed in args? simpler to read config.
    # Assume job_config.json is in parent dir of execution (subdir)
    # OR passed implicitly.
    # Or just hardcode logic to read json?
    
    # Let's rely on job_config.json in WORK_ROOT
    # WORK_ROOT is parent of current execution dir usually? 
    # integrate.sh sets WORK_ROOT.
    
    # Best to pass config path.
    # But for simplicity, let's look in CWD and parent.
    config = {}
    paths = ["job_config.json", "../job_config.json"]
    for p in paths:
        if os.path.exists(p):
            try:
                with open(p, 'r') as f:
                    config = json.load(f)
                break
            except: pass
            
    if not config: return
    
    import redis
    try:
        r = redis.Redis(host=config.get('redis_host'), port=config.get('redis_port'))
        data = {'status': status, 'timestamp': time.time()}
        if msg: data['message'] = msg
        r.set(key, json.dumps(data), ex=604800)
    except:
        pass

if __name__ == "__main__":
    main()
