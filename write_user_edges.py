##
#   void write_user_edges(string, string, string, int[][], dict{})
#   @var referents_out the string path of where to output the transaction_id referents
#   @var public_keys_out the string path of where to output the pubkey_id referents
#   @var edges_out the string path of where to output the main data file (edges)
#   @var user_hash the dictionary mapping pubkeys to user_ids (generated in write_user_public_keys.py)
#   @return None
#
#   This method outputs the files for referent transaction information and main data edges.
# 
#


from operator import itemgetter
import bsddb
def write_user_edges(referents_out, public_keys_out, edges_out, rows, user_hash):
    rf_out = open(referents_out, 'w')
    pk_out = open(public_keys_out, 'w')
    e_out = open(edges_out, 'w')
    rows.sort(key=itemgetter(1,0,2))
    
    pubkey = None
    prev = None
    rf_list = []
    pk_list = []
    #print user_hash.keys()
    i = 0
    for row in rows:
        if row[0] == 0: #if input
            rf_list.append(row[5])
            pk_list.append(row[3])
        
        if row[0] == 0 or prev != row[1]: #if output or mismatch (output can have no input in the case of 'coinbase' mining event, write this as user as input and output, self-loop transaction)
            pubkey = row[3]

        if row[0] == 1: #if output
            if len(rf_list) != 0: #are there referents?
                rf_list.insert(0, row[1])
                rf_out.write(",".join(map(str, map(int, rf_list))) + '\n')
            if len(pk_list) != 0: #are there referents?
                pk_list.insert(0, row[1])
                pk_out.write(",".join(map(str, map(int, pk_list))) + '\n')
            line = [int(row[1]), int(user_hash[pubkey]), int(user_hash[row[3]]), int(row[4]), str(round(float(row[5])*1.0e-8, 7))] # I try to preserve 8 digits of significance here (see: rounding). I've looked closely at getting this precision correct and cant find an incorrect instance. Most transactions are 2 sig. figs.
            e_out.write(",".join(map(str, line)) + '\n') #write to string
            rf_list = []
            pk_list = []
        prev = row[1]
        i += 1 
        if i % 1000000 == 0: #monitor progress
            print "Progress: write user edges, percent complete: " + str(float(i)/len(rows)) 
              
        
    e_out.close()
    pk_out.close()
    rf_out.close()