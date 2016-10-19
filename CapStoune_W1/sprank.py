import sqlite3

conn = sqlite3.connect(r'D:\pagerank\spider.sqlite')
cur = conn.cursor()

# Find the ids that send out page rank - we only are interested
# in pages in the SCC that have in and out links
cur.execute('''SELECT DISTINCT from_id FROM Links''')
from_ids=[row[0] for row in cur]

# Find the ids that receive page rank 
#to_ids = list()
#links = list()
cur.execute('''SELECT DISTINCT from_id, to_id FROM Links''')
links=[row for row in cur if row[0] != row[1] and row[0] in from_ids and row[1] in from_ids]
to_ids=sorted(list({to_id[1] for to_id in links}))
# Get latest page ranks for strongly connected component
prev_ranks = dict()
for node in from_ids:
    cur.execute('''SELECT new_rank FROM Pages WHERE id = ?''', (node, ))
    row = cur.fetchone()
    prev_ranks[node] = row[0]

sval = input('How many iterations:')
many = 1
if ( len(sval) > 0 ) : many = int(sval)

# Sanity check
if len(prev_ranks) < 1 : 
    print ("Nothing to page rank.  Check data.")
    quit()

# Lets do Page Rank in memory so it is really fast
for i in range(many):
    # print prev_ranks.items()[:5]
    print('')
    print('STEP N: ', i)
    print('')
    next_ranks = dict();
    total = 0.0
    for (node, old_rank) in prev_ranks.items():
        total = total + old_rank
        next_ranks[node] = 0.0
    print ('total :', total)

    # Find the number of outbound links and sent the page rank down each
    for (node, old_rank) in prev_ranks.items():
        print ('node : {0}, old_rank : {1}'.format(node, old_rank))
        give_ids = list()
        for (from_id, to_id) in links:
            if from_id != node : continue
            print('   from_ID : {0}, to_ID : {1}'.format(from_id,to_id))

            if to_id not in to_ids: continue
            give_ids.append(to_id)
        if ( len(give_ids) < 1 ) : continue
        amount = old_rank / len(give_ids)
        print ('node : {0}, old_rank : {1}, amount : {2}, give_ids : {3}'.format(node, old_rank,amount, give_ids))
    
        for id in give_ids:
            next_ranks[id] = next_ranks[id] + amount
    
    newtot = 0
    for (node, next_rank) in next_ranks.items():
        newtot = newtot + next_rank
    evap = (total - newtot) / len(next_ranks)

    # print newtot, evap
    for node in next_ranks:
        next_ranks[node] = next_ranks[node] + evap

    newtot = 0
    for (node, next_rank) in next_ranks.items():
        newtot = newtot + next_rank

    # Compute the per-page average change from old rank to new rank
    # As indication of convergence of the algorithm
    totdiff = 0
    for (node, old_rank) in prev_ranks.items():
        new_rank = next_ranks[node]
        diff = abs(old_rank-new_rank)
        totdiff = totdiff + diff

    avediff = totdiff / len(prev_ranks)
    print (i+1, avediff)

    # rotate
    prev_ranks = next_ranks

# Put the final ranks back into the database
print('')
print('='*40)
print ('final operation:')
print('')
print ('next rank :', next_ranks)
print ('next rank items:', list(next_ranks.items())[:5])
print('')

cur.execute('''UPDATE Pages SET old_rank=new_rank''')
for (id, new_rank) in next_ranks.items() :
    cur.execute('''UPDATE Pages SET new_rank=? WHERE id=?''', (new_rank, id))
conn.commit()
cur.close()
print ('all done')
