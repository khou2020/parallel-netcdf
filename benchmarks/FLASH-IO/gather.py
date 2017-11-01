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
                            if (bestrec == None or ('total_time' not in bestrec and 'total_time_max' not in bestrec)):
                                bestrec = rec
                            else:
                                if ('total_time' in bestrec and bestrec['total_time'] > rec['total_time']):
                                    bestrec = rec
                                elif ('total_time_max' in bestrec and bestrec['total_time_max'] > rec['total_time_max']):
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
                        print(line)
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
    dir = 'C:/Users/x3276/OneDrive/Research/Log io/Result/FLASH/64_8M'

    if len(argv) > 1:
        dir = argv[1]
    
    recs = gather(dir)
    
    for rec in recs:
        if 'total_time' in rec and 'total_io_size' in rec:
            rec['total_bandwidth'] = rec['total_io_size'] / rec['total_time'] / 1024

    with open('result.csv', 'w') as fout:
        '''
        filter = {'io_driver': 'ncmpi'}
        plot(fout, recs, filter, 'number_of_processes', 'io_mode', 'total_time')
        filter = {'io_driver': 'bb'}
        plot(fout, recs, filter, 'number_of_processes', 'io_mode', 'total_time')
        filter = {'io_driver': 'stage'}
        plot(fout, recs, filter, 'number_of_processes', 'io_mode', 'total_time')
        filter = {'io_driver': 'ncmpi'}
        plot(fout, recs, filter, 'number_of_processes', 'io_mode', 'total_io_size')
        filter = {'io_driver': 'bb'}
        plot(fout, recs, filter, 'number_of_processes', 'io_mode', 'total_io_size')
        filter = {'io_driver': 'stage'}
        plot(fout, recs, filter, 'number_of_processes', 'io_mode', 'total_io_size')
        filter = {'io_driver': 'ncmpi'}
        plot(fout, recs, filter, 'number_of_processes', 'io_mode', 'total_bandwidth')
        filter = {'io_driver': 'bb'}
        plot(fout, recs, filter, 'number_of_processes', 'io_mode', 'total_bandwidth')
        filter = {'io_driver': 'stage'}
        plot(fout, recs, filter, 'number_of_processes', 'io_mode', 'total_bandwidth')
        filter = {'io_driver': 'bb'}
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_put_time', 'bb_wr_time', 'bb_flush_time', 'bb_rd_time', 'bb_replay_time', 'bb_api_time', 'total_time'])
        filter = {'io_driver': 'stage'}
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'flash_time', 'stage_time'])

        filter = {'experiment': 'bb_timing_breakdown'}
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_total_time_mean', 'bb_create_time_mean', 'bb_enddef_time_mean', 'bb_put_time_mean', 'bb_flush_time_mean', 'bb_close_time_mean'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_total_time_var', 'bb_create_time_var', 'bb_enddef_time_var', 'bb_put_time_var', 'bb_flush_time_var', 'bb_close_time_var'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_total_time_max', 'bb_create_time_max', 'bb_enddef_time_max', 'bb_put_time_max', 'bb_flush_time_max', 'bb_close_time_max'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_total_time_min', 'bb_create_time_min', 'bb_enddef_time_min', 'bb_put_time_min', 'bb_flush_time_min', 'bb_close_time_min'])

        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_put_data_wr_time_mean', 'bb_put_meta_wr_time_mean', 'bb_put_num_wr_time_mean'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_put_data_wr_time_var', 'bb_put_meta_wr_time_var', 'bb_put_num_wr_time_var'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_put_data_wr_time_max', 'bb_put_meta_wr_time_max', 'bb_put_num_wr_time_max'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_put_data_wr_time_min', 'bb_put_meta_wr_time_min', 'bb_put_num_wr_time_min'])

        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_flush_replay_time_mean', 'bb_flush_data_rd_time_mean', 'bb_flush_put_time_mean', 'bb_flush_wait_time_mean'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_flush_replay_time_var', 'bb_flush_data_rd_time_var', 'bb_flush_put_time_var', 'bb_flush_wait_time_var'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_flush_replay_time_max', 'bb_flush_data_rd_time_max', 'bb_flush_put_time_max', 'bb_flush_wait_time_max'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_flush_replay_time_min', 'bb_flush_data_rd_time_min', 'bb_flush_put_time_min', 'bb_flush_wait_time_min'])

        filter = {'io_driver': 'stage', 'io_mode': 'blocking_coll'}
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'flash_time', 'stage_time'])

        filter = {'io_driver': 'stage', 'io_mode': 'blocking_indep'}
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'flash_time', 'stage_time'])

        filter = {'io_driver': 'stage', 'io_mode': 'nonblocking_coll'}
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'flash_time', 'stage_time'])

        filter = {'io_driver': 'stage', 'io_mode': 'nonblocking_indep'}
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'flash_time', 'stage_time'])
        '''

        filter = {'shared_log': 0, 'io_driver': 'bb'}
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'total_time_mean', 'bb_total_time_mean', 'bb_create_time_mean', 'bb_enddef_time_mean', 'bb_put_time_mean', 'bb_flush_time_mean', 'bb_close_time_mean'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'total_time_var', 'bb_total_time_var', 'bb_create_time_var', 'bb_enddef_time_var', 'bb_put_time_var', 'bb_flush_time_var', 'bb_close_time_var'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'total_time_max', 'bb_total_time_max', 'bb_create_time_max', 'bb_enddef_time_max', 'bb_put_time_max', 'bb_flush_time_max', 'bb_close_time_max'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'total_time_min', 'bb_total_time_min', 'bb_create_time_min', 'bb_enddef_time_min', 'bb_put_time_min', 'bb_flush_time_min', 'bb_close_time_min'])

        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_put_data_wr_time_mean', 'bb_put_meta_wr_time_mean', 'bb_put_num_wr_time_mean'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_put_data_wr_time_var', 'bb_put_meta_wr_time_var', 'bb_put_num_wr_time_var'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_put_data_wr_time_max', 'bb_put_meta_wr_time_max', 'bb_put_num_wr_time_max'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_put_data_wr_time_min', 'bb_put_meta_wr_time_min', 'bb_put_num_wr_time_min'])

        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_flush_replay_time_mean', 'bb_flush_data_rd_time_mean', 'bb_flush_put_time_mean', 'bb_flush_wait_time_mean'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_flush_replay_time_var', 'bb_flush_data_rd_time_var', 'bb_flush_put_time_var', 'bb_flush_wait_time_var'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_flush_replay_time_max', 'bb_flush_data_rd_time_max', 'bb_flush_put_time_max', 'bb_flush_wait_time_max'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_flush_replay_time_min', 'bb_flush_data_rd_time_min', 'bb_flush_put_time_min', 'bb_flush_wait_time_min'])

        filter = {'shared_log': 1, 'io_driver': 'bb'}
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'total_time_mean', 'bb_total_time_mean', 'bb_create_time_mean', 'bb_enddef_time_mean', 'bb_put_time_mean', 'bb_flush_time_mean', 'bb_close_time_mean'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'total_time_var', 'bb_total_time_var', 'bb_create_time_var', 'bb_enddef_time_var', 'bb_put_time_var', 'bb_flush_time_var', 'bb_close_time_var'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'total_time_max', 'bb_total_time_max', 'bb_create_time_max', 'bb_enddef_time_max', 'bb_put_time_max', 'bb_flush_time_max', 'bb_close_time_max'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'total_time_min', 'bb_total_time_min', 'bb_create_time_min', 'bb_enddef_time_min', 'bb_put_time_min', 'bb_flush_time_min', 'bb_close_time_min'])

        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_put_data_wr_time_mean', 'bb_put_meta_wr_time_mean', 'bb_put_num_wr_time_mean'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_put_data_wr_time_var', 'bb_put_meta_wr_time_var', 'bb_put_num_wr_time_var'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_put_data_wr_time_max', 'bb_put_meta_wr_time_max', 'bb_put_num_wr_time_max'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_put_data_wr_time_min', 'bb_put_meta_wr_time_min', 'bb_put_num_wr_time_min'])

        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_flush_replay_time_mean', 'bb_flush_data_rd_time_mean', 'bb_flush_put_time_mean', 'bb_flush_wait_time_mean'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_flush_replay_time_var', 'bb_flush_data_rd_time_var', 'bb_flush_put_time_var', 'bb_flush_wait_time_var'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_flush_replay_time_max', 'bb_flush_data_rd_time_max', 'bb_flush_put_time_max', 'bb_flush_wait_time_max'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'bb_flush_replay_time_min', 'bb_flush_data_rd_time_min', 'bb_flush_put_time_min', 'bb_flush_wait_time_min'])

        filter = {'io_driver': 'ncmpi', 'io_mode': 'blocking_coll'}
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'total_time_mean', 'bb_total_time_mean', 'bb_create_time_mean', 'bb_enddef_time_mean', 'bb_put_time_mean', 'bb_flush_time_mean', 'bb_close_time_mean'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'total_time_var', 'bb_total_time_var', 'bb_create_time_var', 'bb_enddef_time_var', 'bb_put_time_var', 'bb_flush_time_var', 'bb_close_time_var'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'total_time_max', 'bb_total_time_max', 'bb_create_time_max', 'bb_enddef_time_max', 'bb_put_time_max', 'bb_flush_time_max', 'bb_close_time_max'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'total_time_min', 'bb_total_time_min', 'bb_create_time_min', 'bb_enddef_time_min', 'bb_put_time_min', 'bb_flush_time_min', 'bb_close_time_min'])
        filter = {'io_driver': 'ncmpi', 'io_mode': 'nonblocking_coll'}
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'total_time_mean', 'bb_total_time_mean', 'bb_create_time_mean', 'bb_enddef_time_mean', 'bb_put_time_mean', 'bb_flush_time_mean', 'bb_close_time_mean'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'total_time_var', 'bb_total_time_var', 'bb_create_time_var', 'bb_enddef_time_var', 'bb_put_time_var', 'bb_flush_time_var', 'bb_close_time_var'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'total_time_max', 'bb_total_time_max', 'bb_create_time_max', 'bb_enddef_time_max', 'bb_put_time_max', 'bb_flush_time_max', 'bb_close_time_max'])
        plot1d(fout, recs, filter, 'number_of_processes', ['total_io_size', 'total_time_min', 'bb_total_time_min', 'bb_create_time_min', 'bb_enddef_time_min', 'bb_put_time_min', 'bb_flush_time_min', 'bb_close_time_min'])

if __name__=='__main__':
    main(sys.argv)