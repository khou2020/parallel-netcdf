import os, sys

def gather(dir:str):
    recs = []
    for filename in os.listdir(dir):
        if filename.endswith(".txt"):
            path = os.path.join(dir, filename)
            rec = {}
            bestrec = None
            drop = False
            with open(path, 'r') as fin:
                for line in fin:
                    if (line[:42] == '--++---+----+++-----++++---+++--+-++--+---'):
                        if (bestrec != None):
                            recs.append(bestrec)
                        bestrec = None
                    elif (line[:40] == '-----+-----++------------+++++++++--+---'):
                        if (not drop):
                            if (bestrec == None or bestrec['io_time'] < rec['io_time']):
                                bestrec = rec
                        rec = {}
                        drop = False
                    elif (line[:3] == '#%$'):
                        tokens = line.split(sep = ':')
                        val = tokens[2].strip()
                        try:
                            int(val)
                            val = int(val)
                        except ValueError:
                            try:
                                float(val)
                                val = float(val)
                            except ValueError:
                                val = val
                        rec[tokens[1].strip()] = val
                    elif (line[:5] == 'Error' or line[:7] == '#Error!'):
                        drop = True
    return recs

def plot(fout, recs:list, filter:dict, x:str, y:str, v:str, highisbetter:bool = False):
    table = {}
    cats = set()
    for rec in recs:
        drop = False
        for k in filter:
            if (k not in rec or rec[k] != filter[k]):
                drop = True
                break
        if (x not in rec or y not in rec or v not in rec):
            drop = True
        if (not drop):
            cats.add(rec[y])
            if (rec[x] not in table):
                table[rec[x]] = {}
            if (rec[y] not in table[rec[x]]):
                table[rec[x]][rec[y]] = rec[v]
            else:
                if (highisbetter):
                    table[rec[x]][rec[y]] = max([table[rec[x]][rec[y]], rec[v]])
                else:
                    table[rec[x]][rec[y]] = min([table[rec[x]][rec[y]], rec[v]])
    
    fout.write(str(v) + ',')
    for f in filter:
        fout.write(f + '=' + str(filter[f]) + '; ')
    fout.write('\n')
    fout.write(str(x) + '\\' + str(y) + ', ')
    for v in sorted(list(cats)):
        fout.write(str(v) + ', ')
    fout.write('\n')
    for u in sorted(table.keys()):
        fout.write(str(u) + ', ')
        for v in sorted(list(cats)):
            if v in table[u]:
                fout.write(str(table[u][v]))
            fout.write(', ')
        fout.write('\n')
    fout.write('\n')

def plot1d(fout, recs:list, filter:dict, x:str, vals:list):
    table = {}
    complete = set()
    for rec in recs:
        drop = False
        for k in filter:
            if (k not in rec or rec[k] != filter[k]):
                drop = True
                break
        if (x not in rec):
            drop = True
                        
        if (not drop):
            if (rec[x] not in table or rec[x] not in complete):
                table[rec[x]] = rec
            for v in vals:
                if (v not in rec):
                    drop = True
            if (not drop):
                complete.add(rec[x])

    
    fout.write(vals[0] + ',')
    for f in filter:
        fout.write(f + '=' + str(filter[f]) + '; ')
    fout.write('\n')
    fout.write(str(x) + ', ')
    for v in vals:
        fout.write(str(v) + ', ')
    fout.write('\n')
    for u in sorted(table.keys()):
        fout.write(str(u) + ', ')
        for v in vals:
            if v in table[u]:
                fout.write(str(table[u][v]))
            fout.write(', ')
        fout.write('\n')
    fout.write('\n')

def main(argv:list):
    dir = ''

    if len(argv) > 1:
        dir = argv[1]
    
    recs = gather(dir)

    for rec in recs:
        if 'io_size' in rec:
            rec['io_size_gb'] = rec['io_size'] / 1024
            if 'io_time' in rec:
                rec['bandwidth_gbps'] = rec['io_size_gb'] / rec['io_time']
                rec['bandwidth'] = rec['io_size'] / rec['io_time']
            if 'bb_rd_time' in rec:
                rec['bb_rd_bandwidth_gbps'] = rec['io_size_gb'] / rec['bb_rd_time']
                rec['bb_rd_bandwidth'] = rec['io_size'] / rec['bb_rd_time']
            if 'pfs_wr_time' in rec:
                rec['pfs_wrbandwidth_gbps'] = rec['io_size_gb'] / rec['pfs_wr_time']
                rec['pfs_wrbandwidth'] = rec['io_size'] / rec['pfs_wr_time']
        if rec['method'] == 'mpi_flush':
            if 'bb_rd_indep' not in rec:
                rec['bb_rd_indep'] = 0
    
    with open('result.csv', 'w') as fout:
        filter = {'method': 'dw_staging'}
        plot(fout, recs, filter, 'io_size_gb', 'method', 'io_time')
        plot(fout, recs, filter, 'io_size_gb', 'method', 'bandwidth_gbps', True)
        filter = {'io_size_gb': 128, 'n_nodes': 32, 'bb_rd_indep': 0}
        plot1d(fout, recs, filter, 'n_proc', ['bb_rd_time', 'pfs_wr_time', 'io_time', 'bb_rd_bandwidth_gbps', 'pfs_wrbandwidth_gbps', 'bandwidth_gbps'])
        filter = {'io_size_gb': 128, 'n_nodes': 32, 'bb_rd_indep': 1}
        plot1d(fout, recs, filter, 'n_proc', ['bb_rd_time', 'pfs_wr_time', 'io_time', 'bb_rd_bandwidth_gbps', 'pfs_wrbandwidth_gbps', 'bandwidth_gbps'])
        filter = {'io_size_gb': 128, 'n_nodes': 64, 'bb_rd_indep': 0}
        plot1d(fout, recs, filter, 'n_proc', ['bb_rd_time', 'pfs_wr_time', 'io_time', 'bb_rd_bandwidth_gbps', 'pfs_wrbandwidth_gbps', 'bandwidth_gbps'])
        filter = {'io_size_gb': 128, 'n_nodes': 64, 'bb_rd_indep': 1}
        plot1d(fout, recs, filter, 'n_proc', ['bb_rd_time', 'pfs_wr_time', 'io_time', 'bb_rd_bandwidth_gbps', 'pfs_wrbandwidth_gbps', 'bandwidth_gbps'])


if __name__=='__main__':
    main(sys.argv)