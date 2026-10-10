#!/usr/bin/env python3
"""Inventory lexical unsafe sites. Not a Rust parser or a soundness checker.

The checked-in inventory requires explicit review when a site changes. Test-only
unsafe and external macros are outside this inventory and retain their own tests.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
ROOT = Path(__file__).resolve().parents[1]
FILES = ['src/berry/raw.rs','src/mosaic/raw.rs','src/mosaic/raw/branches.rs','src/mosaic/raw/remote.rs']


def sites():
    result=[]
    for name in FILES:
        lines=(ROOT/name).read_text().splitlines()
        symbol='';impl='';ordinal=0
        for line_no,line in enumerate(lines,1):
            if line.lstrip().startswith(('//','///')): continue
            m=re.match(r'(?:unsafe )?impl(?:<.*>)? (.+) \{',line)
            if m: impl=m.group(1)
            m=re.search(r'\bfn (\w+)',line)
            if m: symbol=m.group(1)
            if not re.search(r'\bunsafe\s+(?:\{|impl\b|fn\b)',line): continue
            ordinal+=1
            before='\n'.join(lines[max(0,line_no-9):line_no])
            backend='berry' if '/berry/' in name else 'mosaic'
            if '/remote.rs' in name: proof='remote';checks=['mosaic-layout','mosaic-initialized','mosaic-return','branches-lifetime','traits']
            elif '/branches.rs' in name: proof='families';checks=['branches-layout','branches-construction','branches-lifetime','branch-traces','traits']
            elif symbol=='snapshot': proof='observations';checks=['snapshots']
            elif ('Diagnostics' in impl or 'Diagnostics' in line) and symbol not in ['prepare','prepare_payload']: proof='diagnostics';checks=[backend+'-destroy',backend+'-return']
            elif symbol in ['prepare','prepare_payload']: proof='preparation';checks=[backend+'-layout',backend+'-initialized']
            elif symbol=='destroy': proof='teardown';checks=[backend+'-destroy',backend+'-return']
            elif symbol in ['acquire','recycle_domain','open_inner']: proof='admission';checks=[backend+'-admission',backend+'-generation',backend+'-maintenance','reservations']
            elif symbol in ['root','class','domain','slot','free_id','grant','at','word','unassigned','cursor','entry','node','row_id']: proof='layout';checks=[backend+'-layout',backend+'-initialized']
            else: proof='ownership';checks=[backend+'-initialized',backend+'-bytes',backend+'-return','traits']
            result.append(dict(id=f'{name}:{ordinal}',path=name,line=line_no,symbol=impl+'::'+symbol,site=line.strip(),proof=proof,checks=checks,context_sha256=hashlib.sha256(before.encode()).hexdigest()))
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--write',action='store_true');args=p.parse_args()
    path=ROOT/'verification/unsafe-sites.json'
    current=sites()
    if args.write: path.write_text(json.dumps(current,indent=2)+'\n')
    else:
        if not (json.loads(path.read_text())==current): raise RuntimeError('Unsafe-site inventory changed: review obligations and regenerate explicitly.')
        matrix=json.loads((ROOT/'verification/matrix.json').read_text())
        names={x['id'] for x in matrix['checks']}
        proof=(ROOT/'SAFETY_CASE.md').read_text()
        for site in current:
            if not (site['checks'] and set(site['checks'])<=names): raise RuntimeError(site)
            if not (f"### {site['proof']}" in proof): raise RuntimeError(site)
    print(f'{len(current)} unsafe sites inventoried; this is not a proof of soundness')

if __name__=='__main__':main()
