#!/usr/bin/env python

import os
import sys
import time
import getpass
import argparse
import paramiko
import logging
import multiprocessing.dummy as mp
logging.raiseExceptions=False

#worker thread here-----------------------------------------------------------------------------
def get_resources(cx,node,disk_patterns=['/','/data'],verbose=False,rounding=2):
    client=paramiko.SSHClient()
    client.load_system_host_keys()
    client.connect(hostname=cx['host'],port=cx['port'],username=cx['uid'],password=cx['pwd'])
    N = {node:{'cpu':0.0,'mem':0.0,'swap':0.0,'disks':{p:0.0 for p in disk_patterns}}}
    check   = 'top -n 1 | grep "Cpu" && top -n 1 | grep "KiB Mem" && top -n 1 | grep "KiB Swap"'
    check  += ' && '+' && '.join(['df -h | grep %s'%p for p in disk_patterns])
    command = "ssh %s -t '%s'"%(node,check)
    stdin,stdout,stderr=client.exec_command(command,get_pty=True)
    for line in stdout:
        try:
            if line.startswith('%Cpu(s)'):
                idle_cpu = round(100.0-float(line.split(',')[3].split(' ')[1]),rounding)
                N[node]['cpu'] = idle_cpu
            if line.startswith('KiB Mem'):
                total_mem = float(line.split(',')[0].split(' ')[3])
                free_mem  = float(line.split(',')[1].split(' ')[1])
                N[node]['mem'] = round(100.0*(1.0-free_mem/total_mem),rounding)
            if line.startswith('KiB Swap'):
                total_swap = float(line.split(',')[0].split(' ')[4])
                free_swap  = float(line.split(',')[1].split(' ')[3])
                N[node]['swap'] = round(100.0-100.0*(free_swap/total_swap),rounding)
            if line.startswith('/dev/'):
                disk = line.replace('\r','').replace('\n','')
                for p in disk_patterns:
                    if disk.endswith(p):
                        N[node]['disks'][p] = round(float(disk.split(' ')[-2].replace('%','')),rounding)
        except Exception as E:
            N['err'] = E.message
            pass
    client.close()
    return N

def command_runner(cx,node,cmd,verbose=False):
    C = {node:[]}
    client=paramiko.SSHClient()
    client.load_system_host_keys()
    client.connect(hostname=cx['host'],port=cx['port'],username=cx['uid'],password=cx['pwd'])
    if not args.sudo: command = "ssh %s -t '%s'"%(node,cmd)
    else:             command = "ssh %s -t \"echo '%s' | sudo -S %s\""%(node,cx['pwd'],cmd)
    stdin, stdout, stderr = client.exec_command(command)
    for line in stdout: C[node] += [line.replace('\n','')]
    if verbose:
        for line in stderr: C[node] += [line.replace('\n','')]
    client.close()
    return C

#puts data back together
result_list = [] #async queue to put results for || stages
def collect_results(result):
    result_list.append(result)

des = """
---------------------------------------------------------------
multi-node  ssh based multithreaded dispatching client\n11/18/2018-11/30/2018\tTimothy James Becker
---------------------------------------------------------------"""
parser = argparse.ArgumentParser(description=des,formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('--head',type=str,help='hostname of headnode\t\t\t[None]')
parser.add_argument('--port',type=int,help='command port\t\t\t\t[22]')
parser.add_argument('--targets',type=str,help='comma seperated list of host targets\t[/etc/hosts file from head]')
parser.add_argument('--command',type=str,help='command to dispatch\t\t\t[ls -lh]')
parser.add_argument('--sudo',action='store_true',help='elevate the remote dispatched commands\t[False]')
parser.add_argument('--check_resources',action='store_true',help='check cpu,mem,swap,disk resources\t[False]')
parser.add_argument('--verbose',action='store_true',help='output more results to stdout\t\t[False]')
args = parser.parse_args()

if args.head is not None:
    head = args.head.split('.')[0]
    domain = '.'.join(args.head.split('.')[1:])
else:
    raise IOError
if args.port is not None:
    port = args.port
else:
    port = 22
if args.targets is not None:
    nodes = args.targets.split(',')
elif os.path.exists('/etc/hosts'):
    nodes = None
else:
    raise IOError
if args.command is not None:
    cmd = args.command
else:
    cmd = None

if __name__=='__main__':
    paramiko.util.log_to_file("filename.log")
    start = time.time()
    print('user:'),
    uid = sys.stdin.readline().replace('\n','')
    pwd = getpass.getpass(prompt='pwd: ',stream=None).replace('\n','')

    #start it up-----------------------------------------------------------------------
    cx = {'host':head+'.'+domain,'port':port,'uid':uid,'pwd':pwd}
    if nodes is None:
        client=paramiko.SSHClient()
        client.load_system_host_keys()
        client.connect(hostname=cx['host'],port=cx['port'],username=cx['uid'],password=cx['pwd'])
        nodes = []
        stdin,stdout,stderr = client.exec_command('cat /etc/hosts')
        for line in stdout:
            if line.find('::') < 0 and not line.startswith('#') and line != '\n':
                node = line.split(' ')[-1].split('\t')[-1].replace('\n','')
                if node != head: nodes += [node]
        print('using nodes: %s'%nodes)
        client.close()
        time.sleep(0.5)
    res,N,threads = {node:[] for node in nodes},{},len(nodes)
    print('using %s number of connection threads'%threads)
    if args.check_resources: #execute a resource check
        print('checking percent used resources on nodes: %s ..'%nodes)
        #dispatch resource checks to all nodes-------------------------------------
        p1=mp.Pool(threads)
        for node in nodes:  # each site in ||
            p1.apply_async(get_resources,
                           args=(cx,node,['/','/data'],args.verbose,2),
                           callback=collect_results)
            time.sleep(0.1)
        p1.close()
        p1.join()
        #collect results---------------------------------------------------------
        R = {}
        for l in result_list: R[l.keys()[0]] = l[l.keys()[0]]
        for r in R: print('%s: %s'%(r,R[r]))
        result_list = []
    if cmd is not None:
        #dispatch the command to all nodes-------------------------------------
        p1=mp.Pool(threads)
        for node in nodes:  # each site in ||
            print('dispatching work on node : %s ..'%node)
            p1.apply_async(command_runner,
                           args=(cx,node,cmd,args.verbose),
                           callback=collect_results)
            time.sleep(0.1)
        p1.close()
        p1.join()
        #collect results----------------------------------------------------------
        C = {}
        for l in result_list: C[l.keys()[0]] = l[l.keys()[0]]
        for c in C: print('%s: %s'%(c,C[c]))
    #close it down----------------------------------------------------------------------