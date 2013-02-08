##
# long[][] write_network_dictionaries(string, string, string, string)
# @var db_path the string path to write the database files
# @var transactions_in the string path to find the raw transaction file
# @var transaction_keys the string path to write the transaction strings
# @var pub_keys the string path to write the pubkey strings
#
# This method preprocesses the raw bitcoin text data, numerically indexing the lengthy string data and outputting a long int matrix.
#
##

import bsddb
import array as ar
import os
def write_network_dictionaries(db_path, transactions_in, transaction_keys, pub_keys):
    
    #get file length for progress output
    f = open(transactions_in, 'rb')
    file_lines = 0
    for row in f:
        file_lines +=1
    f.close()

    #write out key dictionaries from file
    t_keys = bsddb.hashopen(db_path+'transaction_keys.bdb', 'n', cachesize=2000000)
    p_keys = bsddb.hashopen(db_path+'public_keys.bdb', 'n', cachesize=2000000)
    
    #open files
    tk_out = open(transaction_keys, 'w')
    pk_out = open(pub_keys, 'w')
    f = open(transactions_in, 'rb')
    
    #inits
    t_id = 1
    p_keys["(None)"] = '0'
    pk_out.write("%s\n" % "(None)")
    p_id = 2
    rows = []
    i = 1    
    
    for row in f: #each file line
        add = 1
        fields = row.split()    
        #field specification: ["in", transaction_key, referent_transaction_key, index, public_key, date]   
        if fields[0] == "in" and fields[2] != "coinbase":
            io = 0  #save some space here, should be sorted before 'out'
            tk = fields[2] #actually rfk here for fixing missing pubkeys trick
            idx  = int(fields[3])
            pk = fields[4]
            date = fields[5]
            io_val = fields[2]
            tk_backup = fields[1]
        elif fields[0] == "out":
            io = 1
            tk = fields[1]
            idx  = int(fields[2])
            pk = fields[3]
            date = fields[5]
            tk_backup = fields[1]
            io_val = float(fields[4])*1e8
            tk_backup = fields[1]        
        else:
            add = 0 
                           
        if add == 1:
            date = int(date[0:4] + date[5:7] + date[8:10] + date[11:13] + date[14:16] + date[17:19]) # date to long int to save memory
            if not t_keys.has_key(tk): #if transaction string not indexed (also includes referent transactions)
                t_keys[tk] = str(t_id)
                t_id += 1
                tk_out.write("%s\n" % tk)

            if not t_keys.has_key(tk_backup): #if backup transaction string not indexed
                t_keys[tk_backup] = str(t_id)
                t_id += 1 
                tk_out.write("%s\n" % tk_backup)

            if not p_keys.has_key(pk): #if public key not indexed
                p_keys[pk] = str(p_id)
                p_id += 1    
                pk_out.write("%s\n" % pk)

            if io == 0:
                 io_val = int(t_keys[io_val])       
            rows.append(ar.array('L', map(int, [io, t_keys[tk], idx, p_keys[pk], date, io_val, t_keys[tk_backup]]))) #go to 'L' here to preserve BTC value precision, is corrupted on 4-byte float.
        i += 1
        if i % 1000000 == 0: #print progress
            print "Progress: dictionary, percent complete: " + str(float(i)/file_lines)
    p_keys.close()
    t_keys.close()    
    f.close()
    tk_out.close()
    pk_out.close()
    os.remove(db_path+'public_keys.bdb')
    os.remove(db_path+'transaction_keys.bdb')
    return rows;