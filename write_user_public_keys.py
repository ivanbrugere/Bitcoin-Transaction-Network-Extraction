##
#   dict{} = write_user_public_keys(string, int[][])
#   @var users_out the string path where to write the 'user' groupings
#   @var rows the main data matrix
#   @return the dictionary mapping pubkeys to grouping numbers

#   This method writes the groupings of pubkeys given their use as shared inputs to a transaction it returns a dictionary where d[key] := "the line number to find the pubkey in the outputted file."
#

from operator import itemgetter
def write_user_public_keys(users_out, rows):
    
    #build shared keys based on input rows
    v = []
    for row in rows:
        if row[0] == 0: #is input?
            v.append([row[1], row[3]])
    v.sort(key=itemgetter(0,1)) #sort on [transaction_id, pub_key]
    
    #single-link process: create 'edge' (tuple) between a seed and each other pubkey in transaction
    seed = v[0]
    prev = None
    keys = []
    v_out = []
    for r in v:
        if r[0] == prev: #match transaction with seed
            if seed != r[1]: #if this is not seed
                v_out.append([seed, r[1]]) 
        else:    
            seed = r[1]
        prev = r[0]
    v_out.sort(key=itemgetter(0))
    v = v_out
    v_out = None

    #merge the pairs generated above
    d = {}
    d_list = []
    current_cluster = 0
    i = 0
    for row in v: #for each edge tuple
        r0 = d.has_key(row[0]) #check key0
        r1 = d.has_key(row[1]) #check key1
        if not r0 and not r1: #neither exists: add new group
            a = len(d_list); 
            d[row[0]] = current_cluster
            d[row[1]] = current_cluster
            d_list.append([row[0], row[1]])
            current_cluster += 1
        elif not r0 and r1: #key1 exists, assign key0 = key1
            d[row[0]] = d[row[1]]       
            d_list[d[row[1]]].append(row[0]) 
        
        elif r0 and not r1: #key0 exists, assign key1 = key0
            d[row[1]] = d[row[0]]
            d_list[d[row[0]]].append(row[1])
        elif r0 and r1 and d[row[0]] != d[row[1]]: #key1 and key0 exist and distinct, (join), assign the minimum as the joined group
            min_value = min(d[row[0]], d[row[1]])
            max_value = max(d[row[0]], d[row[1]])
        
            for key in d_list[max_value]: #this is why we need to maintain the d_list, so we can reassign keys quickly. 
                d[key] = min_value
        
            d_list[min_value] = list(set(d_list[min_value] + d_list[max_value]))
            d_list[max_value] = []
        i +=1
        if i % 1000000 == 0:
            print "Progress: merging shared keys, percent complete: " + str(float(i)/len(v))
        
    #cleanup
    d_list = None
    keys = d.keys()
    values = d.values()
    d = {}    
    
    #output merged keys
    idx = sorted(range(len(values)), key = values.__getitem__) #get indicies of sorted cluster_ids, to output pubkeys in cluster_id sorted order. #implements numpy.argsort(), from http://stackoverflow.com/questions/3382352/equivalent-of-numpy-argsort-in-basic-python
    thefile = open(users_out, 'w')
    prev = ''
    s = ''
    current_cluster = 1
    for index in idx:
        if prev == values[index]:
            s += ',' + str(int(keys[index]))
            d[int(keys[index])] = current_cluster #we want to return a complete dictionary
        else:
            if s != '':
                thefile.write("%s\n" % s)
                current_cluster += 1 #note that we are re-indexing as we go, because there are 'holes' in the numbering from merges
            s = str(int(keys[index]))
            d[int(keys[index])] = current_cluster
            #print [keys[index], current_cluster] 
        prev = values[index]
    
    if s != '': #trailing case
         thefile.write("%s\n" % s)
         current_cluster+= 1

    #non-shared merged keys: we need to index pubkeys that didn't have a pairing
    v = []
    for row in rows:
        v.append(row[3])
    v = set(v)
    unshared_keys = v.difference(set(keys))
    
    #output and index them in the dictionary    
    for item in unshared_keys:
      d[int(item)] = current_cluster  
      current_cluster += 1
      thefile.write("%s\n" % int(item))
    thefile.close()
    return d