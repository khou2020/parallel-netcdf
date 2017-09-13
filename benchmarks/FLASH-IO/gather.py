import os, sys

def gather(dir:str):
    recs = []
    for filename in os.listdir(dir):
        if filename.endswith(".txt"):
            path = os.path.join(dir, filename)
            with open(path, 'r') as fin:
                rec = {}
                log_metadata_size = 0
                log_data_size = 0
                log_flush_buffer_size = 0
                log_put_time = 0.0
                log_bb_wr_time = 0.0
                log_bb_rd_time = 0.0
                log_flush_time = 0.0
                log_replay_time = 0.0
                log_api_time = 0.0
                rec['File_Name'] = filename
                rec['Stage_Out_Time'] = 0.0
                for line in fin:
                    line = str(line)
                    if 'IO_DRIVER:' in line:
                        tokens = line.split(sep = ':')
                        rec['IO_Driver'] = tokens[-1].strip()
                    elif 'IO_MODE:' in line:
                        tokens = line.split(sep = ':')
                        rec['IO_Mode'] = tokens[-1].strip()
                    elif 'N_NODE:' in line:
                        tokens = line.split(sep = ':')
                        rec['N_Node'] = int(tokens[-1].strip())
                    elif 'N_PROC:' in line:
                        tokens = line.split(sep = ':')
                        rec['N_Proc'] = int(tokens[-1].strip())
                    elif 'cb_nodes, value =' in line:
                        tokens = line.split(sep = '=')
                        rec['Cb_Nodes'] = int(tokens[-1].strip())
                    elif 'Max I/O  time=' in line:
                        tokens = line.split(sep = '=')
                        rec['GCRM_IO_Time'] = float(tokens[-1].strip()[:-3])
                    elif 'put time=' in line:
                        tokens = line.split(sep = '=')
                        if (not ('GCRM_Put_Time' in rec)):
                            rec['GCRM_Put_Time'] = float(tokens[-1].strip()[:-3])
                    elif 'close time=' in line:
                        tokens = line.split(sep = '=')
                        if (not ('GCRM_Close_Time' in rec)):
                            rec['GCRM_Close_Time'] = float(tokens[-1].strip()[:-3])
                    elif 'Write  amount=' in line:
                        tokens = line.split(sep = '=')
                        rec['GCRM_Write_Size'] = float(tokens[-1].strip()[:-3])
                    elif 'Read   amount=' in line:
                        tokens = line.split(sep = '=')
                        rec['GCRM_Read_Size'] = float(tokens[-1].strip()[:-3])
                    elif 'I/O    amount=' in line:
                        tokens = line.split(sep = '=')
                        rec['GCRM_IO_Size'] = float(tokens[-1].strip()[:-3])
                    elif 'I/O bandwidth=' in line:
                        tokens = line.split(sep = '=')
                        rec['GCRM_Bandwidth'] = float(tokens[-1].strip()[:-7])      
                    elif 'I/O bandwidth=' in line:
                        tokens = line.split(sep = '=')
                        rec['GCRM_Bandwidth'] = float(tokens[-1].strip()[:-7])
                    elif 'Time in log:' in line:
                        tokens = line.split(sep = ':')
                        log_api_time += float(tokens[-1].strip())   
                    elif 'Time recording entries:' in line:
                        tokens = line.split(sep = ':')
                        log_put_time += float(tokens[-1].strip())   
                    elif 'Time flushing:' in line:
                        tokens = line.split(sep = ':')
                        log_flush_time += float(tokens[-1].strip())    
                    elif 'Time writing to BB:' in line:
                        tokens = line.split(sep = ':')
                        log_bb_wr_time += float(tokens[-1].strip())  
                    elif 'Time reading from BB:' in line:
                        tokens = line.split(sep = ':')
                        log_bb_rd_time += float(tokens[-1].strip())  
                    elif 'Time replaying:' in line:
                        tokens = line.split(sep = ':')
                        log_replay_time += float(tokens[-1].strip())     
                    elif 'Metadata generated:' in line:
                        tokens = line.split(sep = ':')
                        log_metadata_size += float(tokens[-1].strip())  
                    elif 'Data writen to variable:' in line:
                        tokens = line.split(sep = ':')
                        log_data_size += float(tokens[-1].strip())  
                    elif 'Flush buffer size:' in line:
                        tokens = line.split(sep = ':')
                        bsize = float(tokens[-1].strip()) 
                        if (log_flush_buffer_size < bsize):
                            log_flush_buffer_size = bsize
                    elif 'Time stagging:' in line:
                        tokens = line.split(sep = ':')
                        rec['Stage_Out_Time'] = float(tokens[-1].strip()[:-3])
                        
                    
                    rec['Log_Api_Time'] = log_api_time
                    rec['Log_Metadata_Size'] = log_metadata_size
                    rec['Log_Data_Size'] = log_data_size
                    rec['Log_BB_Write_Time'] = log_bb_wr_time
                    rec['Log_BB_Read_Time'] = log_bb_rd_time
                    rec['Log_Flush_Time'] = log_flush_time
                    rec['Log_Replay_Time'] = log_replay_time
                    rec['Log_Put_Time'] = log_put_time
                    rec['Log_Buffer_Size'] = log_flush_buffer_size

            recs.append(rec)
    return recs

def main(argv:list):
    dir = '.'

    if len(argv) > 1:
        dir = argv[1]
    
    recs = gather(dir)

    Total_IO_Time = {}
    IO_Time = {}
    Put_Time = {}
    Close_Time = {}
    Log_Flush_Time = {}
    Log_Put_Time = {}
    Log_Write_Time = {}
    Log_Read_Time = {}
    Log_Replay_Time = {}
    Stage_Out_Time = {}

    for rec in recs:
        ylbl = rec['IO_Driver'] + '_' + rec['IO_Mode']
        xlbl = rec['N_Proc']
        if not (ylbl in IO_Time):
            Total_IO_Time[ylbl] = {}
            IO_Time[ylbl] = {}
            Put_Time[ylbl] = {}
            Close_Time[ylbl] = {}
            Log_Flush_Time[ylbl] = {}
            Log_Put_Time[ylbl] = {}
            Stage_Out_Time[ylbl] = {}
            Log_Replay_Time[ylbl] = {}
            Log_Write_Time[ylbl] = {}
            Log_Read_Time[ylbl] = {}
        Total_IO_Time[ylbl][xlbl] = rec['GCRM_IO_Time'] + rec['Stage_Out_Time']
        IO_Time[ylbl][xlbl] = rec['GCRM_IO_Time']
        Put_Time[ylbl][xlbl] = rec['GCRM_Put_Time']
        Close_Time[ylbl][xlbl] = rec['GCRM_Close_Time']
        Log_Flush_Time[ylbl][xlbl] = rec['Log_Flush_Time']
        Log_Put_Time[ylbl][xlbl] = rec['Log_Put_Time']
        Stage_Out_Time[ylbl][xlbl] = rec['Stage_Out_Time']
        Log_Replay_Time[ylbl][xlbl] = rec['Log_Replay_Time']
        Log_Write_Time[ylbl][xlbl] = rec['Log_BB_Write_Time']
        Log_Read_Time[ylbl][xlbl] = rec['Log_BB_Read_Time']
    
    with open(dir + '/result.csv', 'w') as fout:
        fout.write('Total_IO_Time, 32, 64, 128, 256, 512, 1024\n')
        for y in IO_Time:
            fout.write(y + ', ')
            for p in [32, 64, 128, 256, 512, 1024]:
                if (p in IO_Time[y]):
                    fout.write(str(Total_IO_Time[y][p]) + ', ')
                else:
                    fout.write('-, ')
            fout.write('\n')
        fout.write(',,,,,,\n')
        fout.write('IO_Time, 32, 64, 128, 256, 512, 1024\n')
        for y in IO_Time:
            fout.write(y + ', ')
            for p in [32, 64, 128, 256, 512, 1024]:
                if (p in IO_Time[y]):
                    fout.write(str(IO_Time[y][p]) + ', ')
                else:
                    fout.write('-, ')
            fout.write('\n')
        fout.write(',,,,,,\n')
        fout.write('Put_Time, 32, 64, 128, 256, 512, 1024\n')
        for y in IO_Time:
            fout.write(y + ', ')
            for p in [32, 64, 128, 256, 512, 1024]:
                if (p in IO_Time[y]):
                    fout.write(str(Put_Time[y][p]) + ', ')
                else:
                    fout.write('-, ')
            fout.write('\n')
        fout.write(',,,,,,\n')
        fout.write('Close_Time, 32, 64, 128, 256, 512, 1024\n')
        for y in IO_Time:
            fout.write(y + ', ')
            for p in [32, 64, 128, 256, 512, 1024]:
                if (p in IO_Time[y]):
                    fout.write(str(Close_Time[y][p]) + ', ')
                else:
                    fout.write('-, ')
            fout.write('\n')
        fout.write(',,,,,,\n')
        fout.write('Log_Flush_Time, 32, 64, 128, 256, 512, 1024\n')
        for y in IO_Time:
            fout.write(y + ', ')
            for p in [32, 64, 128, 256, 512, 1024]:
                if (p in IO_Time[y]):
                    fout.write(str(Log_Flush_Time[y][p]) + ', ')
                else:
                    fout.write('-, ')
            fout.write('\n')
        fout.write(',,,,,,\n')
        fout.write('Log_Put_Time, 32, 64, 128, 256, 512, 1024\n')
        for y in IO_Time:
            fout.write(y + ', ')
            for p in [32, 64, 128, 256, 512, 1024]:
                if (p in IO_Time[y]):
                    fout.write(str(Log_Put_Time[y][p]) + ', ')
                else:
                    fout.write('-, ')
            fout.write('\n')
        fout.write(',,,,,,\n')
        fout.write('Stage_Out_Time, 32, 64, 128, 256, 512, 1024\n')
        for y in IO_Time:
            fout.write(y + ', ')
            for p in [32, 64, 128, 256, 512, 1024]:
                if (p in IO_Time[y]):
                    fout.write(str(Stage_Out_Time[y][p]) + ', ')
                else:
                    fout.write('-, ')
            fout.write('\n')
        fout.write(',,,,,,\n')
        fout.write('Log_Replay_Time, 32, 64, 128, 256, 512, 1024\n')
        for y in IO_Time:
            fout.write(y + ', ')
            for p in [32, 64, 128, 256, 512, 1024]:
                if (p in IO_Time[y]):
                    fout.write(str(Log_Replay_Time[y][p]) + ', ')
                else:
                    fout.write('-, ')
            fout.write('\n')
        fout.write(',,,,,,\n')
        fout.write('BB_Write_Time, 32, 64, 128, 256, 512, 1024\n')
        for y in IO_Time:
            fout.write(y + ', ')
            for p in [32, 64, 128, 256, 512, 1024]:
                if (p in IO_Time[y]):
                    fout.write(str(Log_Write_Time[y][p]) + ', ')
                else:
                    fout.write('-, ')
            fout.write('\n')
        fout.write(',,,,,,\n')
        fout.write('BB_Read_Time, 32, 64, 128, 256, 512, 1024\n')
        for y in IO_Time:
            fout.write(y + ', ')
            for p in [32, 64, 128, 256, 512, 1024]:
                if (p in IO_Time[y]):
                    fout.write(str(Log_Read_Time[y][p]) + ', ')
                else:
                    fout.write('-, ')
            fout.write('\n')

if __name__=='__main__':
    main(sys.argv)