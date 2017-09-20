import os, sys

def gather(dir:str):
    recs = []
    for filename in os.listdir(dir):
        if filename.endswith(".txt"):
            path = os.path.join(dir, filename)
            with open(path, 'r') as fin:
                rec = {}
                log_put_time = 1000000.0
                log_bb_wr_time = 1000000.0
                log_bb_rd_time = 1000000.0
                log_flush_time = 1000000.0
                log_replay_time = 1000000.0
                log_api_time = 1000000.0
                rec['Log_Put_Time'] = 1000000.0
                rec['Log_BB_Write_Time'] = 1000000.0
                rec['Log_BB_Read_Time'] = 1000000.0
                rec['Log_Flush_Time'] = 1000000.0
                rec['Log_Replay_Time'] = 1000000.0
                rec['Log_Api_Time'] = 1000000.0
                rec['Stage_Out_Time'] = 1000000.0
                rec['File_Name'] = filename
                for line in fin:
                    line = str(line)
                    tokens = line.split(sep = ':')
                    if 'Round' in line:
                        rec['Log_Put_Time'] = min([rec['Log_Put_Time'], log_put_time])
                        rec['Log_BB_Write_Time'] = min([rec['Log_BB_Write_Time'], log_bb_wr_time])
                        rec['Log_BB_Read_Time'] = min([rec['Log_BB_Read_Time'], log_bb_rd_time])
                        rec['Log_Flush_Time'] = min([rec['Log_Flush_Time'], log_flush_time])
                        rec['Log_Replay_Time'] = min([rec['Log_Replay_Time'], log_replay_time])
                        rec['Log_Api_Time'] = min([rec['Log_Api_Time'], log_api_time])
                          
                        log_metadata_size = 0
                        log_data_size = 0
                        log_flush_buffer_size = 0
                        log_put_time = 0.0
                        log_bb_wr_time = 0.0
                        log_bb_rd_time = 0.0
                        log_flush_time = 0.0
                        log_replay_time = 0.0
                        log_api_time = 0.0           
                    elif 'cb_nodes, value =' in line:
                        tokens = line.split(sep = '=')
                        rec['Cb_Nodes'] = int(tokens[-1].strip())
                    elif 'IO_DRIVER:' in line:
                        rec['IO_Driver'] = tokens[-1].strip()
                    elif 'IO_MODE:' in line:
                        rec['IO_Mode'] = tokens[-1].strip()
                    elif 'N_NODE:' in line:
                        rec['N_Node'] = int(tokens[-1].strip())
                    elif 'N_PROC:' in line:
                        rec['N_Proc'] = int(tokens[-1].strip())

                    elif 'Total I/O amount' in line:
                        rec['Flash_IO_Size'] = float(tokens[-1].strip()[:-3])   
                    elif 'wall time' in line:
                        if 'Flash_Wall_Time' in rec:
                            rec['Flash_Wall_Time'] = min([rec['Flash_Wall_Time'], float(tokens[-1].strip()[:-3])])
                        else:
                            rec['Flash_Wall_Time'] = float(tokens[-1].strip()[:-3])
                    elif 'bandwidth   :' in line:
                        if 'Flash_Bandwidth' in rec:
                            rec['Flash_Bandwidth'] = min([rec['Flash_Bandwidth'], float(tokens[-1].strip()[:-3])])
                        else:
                            rec['Flash_Bandwidth'] = float(tokens[-1].strip()[:-3])
                    elif 'checkpoint time' in line:
                        if 'Flash_Checkpoint_Time' in rec:
                            rec['Flash_Checkpoint_Time'] = min([rec['Flash_Checkpoint_Time'], float(tokens[-1].strip()[:-3])])
                        else:
                            rec['Flash_Checkpoint_Time'] = float(tokens[-1].strip()[:-3])
                    elif 'checkpoint max header' in line:
                        if 'Flash_Checkpoint_Header_Time' in rec:
                            rec['Flash_Checkpoint_Header_Time'] = min([rec['Flash_Checkpoint_Header_Time'], float(tokens[-1].strip()[:-3])])
                        else:
                            rec['Flash_Checkpoint_Header_Time'] = float(tokens[-1].strip()[:-3])
                    elif 'checkpoint max unknown' in line:
                        if 'Flash_Checkpoint_Other_Time' in rec:
                            rec['Flash_Checkpoint_Other_Time'] = min([rec['Flash_Checkpoint_Other_Time'], float(tokens[-1].strip()[:-3])])
                        else:
                            rec['Flash_Checkpoint_Other_Time'] = float(tokens[-1].strip()[:-3])
                    elif 'checkpoint max close' in line:
                        if 'Flash_Checkpoint_Close_Time' in rec:
                            rec['Flash_Checkpoint_Close_Time'] = min([rec['Flash_Checkpoint_Close_Time'], float(tokens[-1].strip()[:-3])])
                        else:
                            rec['Flash_Checkpoint_Close_Time'] = float(tokens[-1].strip()[:-3])
                    elif 'checkpoint I/O amount' in line:
                        if 'Flash_Checkpoint_IO_Size' in rec:
                            rec['Flash_Checkpoint_IO_Size'] = min([rec['Flash_Checkpoint_IO_Size'], float(tokens[-1].strip()[:-3])])
                        else:
                            rec['Flash_Checkpoint_IO_Size'] = float(tokens[-1].strip()[:-3])

                    elif 'Time in log:' in line:
                        log_api_time += float(tokens[-1].strip())   
                    elif 'Time recording entries:' in line:
                        log_put_time += float(tokens[-1].strip())   
                    elif 'Time flushing:' in line:
                        log_flush_time += float(tokens[-1].strip())    
                    elif 'Time writing to BB:' in line:
                        log_bb_wr_time += float(tokens[-1].strip())  
                    elif 'Time reading from BB:' in line:
                        log_bb_rd_time += float(tokens[-1].strip())  
                    elif 'Time replaying:' in line:
                        log_replay_time += float(tokens[-1].strip())     
                    elif 'Metadata generated:' in line:
                        log_metadata_size += float(tokens[-1].strip())  
                    elif 'Data writen to variable:' in line:
                        log_data_size += float(tokens[-1].strip())  
                    elif 'Flush buffer size:' in line:
                        bsize = float(tokens[-1].strip()) 
                        if (log_flush_buffer_size < bsize):
                            log_flush_buffer_size = bsize


                    elif 'Time stagging:' in line:
                        rec['Stage_Out_Time'] = min([rec['Stage_Out_Time'], float(tokens[-1].strip()[:-3])])
                        
            rec['Log_Metadata_Size'] = log_metadata_size
            rec['Log_Data_Size'] = log_data_size
            rec['Log_Buffer_Size'] = log_flush_buffer_size
            rec['Log_Put_Time'] = min([rec['Log_Put_Time'], log_put_time])
            rec['Log_BB_Write_Time'] = min([rec['Log_BB_Write_Time'], log_bb_wr_time])
            rec['Log_BB_Read_Time'] = min([rec['Log_BB_Read_Time'], log_bb_rd_time])
            rec['Log_Flush_Time'] = min([rec['Log_Flush_Time'], log_flush_time])
            rec['Log_Replay_Time'] = min([rec['Log_Replay_Time'], log_replay_time])
            rec['Log_Api_Time'] = min([rec['Log_Api_Time'], log_api_time])

            if rec['Log_Put_Time'] >= 1000000:
                rec['Log_Put_Time'] = 0
            if rec['Log_BB_Write_Time'] >= 1000000:
                rec['Log_BB_Write_Time'] = 0
            if rec['Log_BB_Read_Time'] >= 1000000:
                rec['Log_BB_Read_Time'] = 0
            if rec['Log_Flush_Time'] >= 1000000:
                rec['Log_Flush_Time'] = 0
            if rec['Log_Replay_Time'] >= 1000000:
                rec['Log_Replay_Time'] = 0
            if rec['Log_Api_Time'] >= 1000000:
                rec['Log_Api_Time'] = 0
            if rec['Stage_Out_Time'] >= 1000000:
                rec['Stage_Out_Time'] = 0

            recs.append(rec)
    return recs

def main(argv:list):
    dir = '.'

    if len(argv) > 1:
        dir = argv[1]
    
    recs = gather(dir)

    Total_IO_Time = {}
    Wall_Time = {}
    IO_Size = {}
    Log_Flush_Time = {}
    Log_Put_Time = {}
    Log_Write_Time = {}
    Log_Read_Time = {}
    Log_Replay_Time = {}
    Stage_Out_Time = {}

    for rec in recs:
        ylbl = rec['IO_Driver'] + '_' + rec['IO_Mode']
        xlbl = rec['N_Proc']
        if not (ylbl in Total_IO_Time):
            Total_IO_Time[ylbl] = {}
            Wall_Time[ylbl] = {}
            IO_Size[ylbl] = {}
            Log_Flush_Time[ylbl] = {}
            Log_Put_Time[ylbl] = {}
            Stage_Out_Time[ylbl] = {}
            Log_Replay_Time[ylbl] = {}
            Log_Write_Time[ylbl] = {}
            Log_Read_Time[ylbl] = {}
        Total_IO_Time[ylbl][xlbl] = rec['Flash_Wall_Time'] + rec['Stage_Out_Time']
        Wall_Time[ylbl][xlbl] = rec['Flash_Wall_Time']
        IO_Size[ylbl][xlbl] = rec['Flash_IO_Size']
        Log_Flush_Time[ylbl][xlbl] = rec['Log_Flush_Time']
        Log_Put_Time[ylbl][xlbl] = rec['Log_Put_Time']
        Stage_Out_Time[ylbl][xlbl] = rec['Stage_Out_Time']
        Log_Replay_Time[ylbl][xlbl] = rec['Log_Replay_Time']
        Log_Write_Time[ylbl][xlbl] = rec['Log_BB_Write_Time']
        Log_Read_Time[ylbl][xlbl] = rec['Log_BB_Read_Time']
    
    with open(dir + '/flash.csv', 'w') as fout:
        fout.write('Total_IO_Time, 32, 64, 128, 256, 512, 1024\n')
        for y in Total_IO_Time:
            fout.write(y + ', ')
            for p in [32, 64, 128, 256, 512, 1024]:
                if (p in Total_IO_Time[y]):
                    fout.write(str(Total_IO_Time[y][p]) + ', ')
                else:
                    fout.write('-, ')
            fout.write('\n')
        fout.write(',,,,,,\n')
        fout.write('FLASH_Wall_Time, 32, 64, 128, 256, 512, 1024\n')
        for y in Total_IO_Time:
            fout.write(y + ', ')
            for p in [32, 64, 128, 256, 512, 1024]:
                if (p in Total_IO_Time[y]):
                    fout.write(str(Wall_Time[y][p]) + ', ')
                else:
                    fout.write('-, ')
            fout.write('\n')
        fout.write(',,,,,,\n')
        fout.write('IO_Size, 32, 64, 128, 256, 512, 1024\n')
        for y in Total_IO_Time:
            fout.write(y + ', ')
            for p in [32, 64, 128, 256, 512, 1024]:
                if (p in Total_IO_Time[y]):
                    fout.write(str(IO_Size[y][p]) + ', ')
                else:
                    fout.write('-, ')
            fout.write('\n')
        fout.write(',,,,,,\n')
        fout.write('Log_Flush_Time, 32, 64, 128, 256, 512, 1024\n')
        for y in Total_IO_Time:
            fout.write(y + ', ')
            for p in [32, 64, 128, 256, 512, 1024]:
                if (p in Total_IO_Time[y]):
                    fout.write(str(Log_Flush_Time[y][p]) + ', ')
                else:
                    fout.write('-, ')
            fout.write('\n')
        fout.write(',,,,,,\n')
        fout.write('Log_Put_Time, 32, 64, 128, 256, 512, 1024\n')
        for y in Total_IO_Time:
            fout.write(y + ', ')
            for p in [32, 64, 128, 256, 512, 1024]:
                if (p in Total_IO_Time[y]):
                    fout.write(str(Log_Put_Time[y][p]) + ', ')
                else:
                    fout.write('-, ')
            fout.write('\n')
        fout.write(',,,,,,\n')
        fout.write('Stage_Out_Time, 32, 64, 128, 256, 512, 1024\n')
        for y in Total_IO_Time:
            fout.write(y + ', ')
            for p in [32, 64, 128, 256, 512, 1024]:
                if (p in Total_IO_Time[y]):
                    fout.write(str(Stage_Out_Time[y][p]) + ', ')
                else:
                    fout.write('-, ')
            fout.write('\n')
        fout.write(',,,,,,\n')
        fout.write('Log_Replay_Time, 32, 64, 128, 256, 512, 1024\n')
        for y in Total_IO_Time:
            fout.write(y + ', ')
            for p in [32, 64, 128, 256, 512, 1024]:
                if (p in Total_IO_Time[y]):
                    fout.write(str(Log_Replay_Time[y][p]) + ', ')
                else:
                    fout.write('-, ')
            fout.write('\n')
        fout.write(',,,,,,\n')
        fout.write('BB_Write_Time, 32, 64, 128, 256, 512, 1024\n')
        for y in Total_IO_Time:
            fout.write(y + ', ')
            for p in [32, 64, 128, 256, 512, 1024]:
                if (p in Total_IO_Time[y]):
                    fout.write(str(Log_Write_Time[y][p]) + ', ')
                else:
                    fout.write('-, ')
            fout.write('\n')
        fout.write(',,,,,,\n')
        fout.write('BB_Read_Time, 32, 64, 128, 256, 512, 1024\n')
        for y in Total_IO_Time:
            fout.write(y + ', ')
            for p in [32, 64, 128, 256, 512, 1024]:
                if (p in Total_IO_Time[y]):
                    fout.write(str(Log_Read_Time[y][p]) + ', ')
                else:
                    fout.write('-, ')
            fout.write('\n')

if __name__=='__main__':
    main(sys.argv)