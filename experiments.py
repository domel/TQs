import io
import os
import csv
import sys
import glob
import random
import signal
import datetime
import subprocess

#
# This scripts runs an number of experiments that aim at finding the running
# times of various queries on PostgreSQL. It is intended to be run in relative
# isolation i.e., with no other time consuming task being performed on the
# machine. Consequently, the script uses a single PostgreSQL database and
# creates a number of temporary files (small in the current directory and larger
# ones in a temporary directory). The script uses process control to implement
# timeout functionality: terminating an ongoing query execution. Because of
# client-server architecture of PostgeSQL simple but relatively costly
# mechanisms for termination has been implemented (recreating the whole
# database). 
#
# REQUIREMENTS: 
# 1. The script must be run from a UNIX shell environment that has
#    the commands `psql`, `createdb`, and `dropdb` in its search path. 
#    It has been only tested with PostgreSQL ver. 12.8, PostgreSQL ver. 13.4 
#    and PostgreSQL ver. 14.0
# 2. It has been only tested with Python ver. 3.9.7 and Python ver. 3.8.10
# 
# Authors: Sławek Staworko <slawomir.staworko@univ-lille.fr>
#          Dominik Tomaszuk <d.tomaszuk@uwb.edu.pl>
#          Filip Murlak <fmurlak@mimuw.edu.pl >
#

default_runs = 3
default_indexed = False
default_timeout = 1800000 # in milliseconds
default_hard_timeout = True # break if the first result is timeout

load_script_file = 'load-script.sql'
query_script_file = 'query-script.sql'
query_script_log = 'query-script.log'
tmp_dir = '/tmp/'
results_dir = 'results'
r_file = tmp_dir + 'r.csv'
db_name = 'threshold' 
pg_port = 5433

movie_link_file = '/home/user/movie_link.csv' # dir for movie_link.csv


if not os.path.exists(results_dir):
    os.mkdir(results_dir)

if not os.path.isdir(results_dir):
    print('Error:', results_dir,'already exists but is not a directory')
    exit()

#
# Experiment specification:
# * Data description
# * Query description
# * Experiment parameters
#   * number of runs to perform
#   * timeout (threshold after which an experiment is to be terminated, in
#              miliseconds)
#
# exp : {
#   'data'    : data,
#   'query'   : query,
#   'runs'    : int,
#   'timeout' : int,
# }
#
#
# Data information (some values are input parameters for  generating
# methods, while others are calculated) :
# * kind of data [barabasi albert (ba), imdb, full]
# * number of nodes (n)          [input param for ba, full]
# * out degree      (m0)         [input param for ba] 
# * imdb link types (link-types) [optional input param for imdb]
# * number of edges (m)          [calculated for ba, imdb, full]
# * indexing        (indexed)    [input param for ba, imdb, full]
# 
# The parameter link-types is a collection if link type identifiers (ints). If
# the link-types is missing for imdb then all types of links are used.
#
# data : {
#    'kind'      : {'ba', 'full', 'imdb'}
#    'n'         : int, 
#    'm0'        : int,
#    'link-types : int+, 
#    'm'         : int,
# }
#
# 
# Query specification:
# * query class (TQ1, TQ2, TQ3)
# * query method (naive or windowed)
# * number of joins (k)
# * threshold value 
#
#
# Abstract query description
# query = {
#    'class'     : {'TQ1','TQ2','TQ3'}
#    'method'    : {'naive','windowed'}
#    'k'         : int
#    'threshold' : int 
# }
#

#
# Returns simple description of generated data
#
def descr_data(data):
    
    kind = data['kind']
    if kind == 'imdb':
        descr = 'imdb data'
        link_types = data.get('link_types')
        descr += ' with link types ' + descr_link_types(link_types)
        descr += ' size ' + str(calc_imdb_size(link_types))
    elif kind == 'ba':
        descr = 'barabasi albert for n='+str(data['n'])+' and m0='+str(data['m0'])
    elif kind == 'full':
        descr = 'full graph with n='+str(data['n'])
    else:
        print('Error: unsupported data kind',kind)
        exit(1)

    if data.get('indexed'):
        descr += ' indexed'
    else:
        descr += ' no index'
    return descr

#
# Prepares a script for loading data into PostgreSQL database.  
# 
# The procedures for each kind of data also set an appropriate value of
# data['m'].
# 
def prepare_data(data):
    kind = data['kind']

    print('Preparing', descr_data(data))

    if kind == 'imdb':
        prepare_imdb_data(data)
    elif kind == 'ba':
        generate_barabasi_albert_data(data)
    elif kind == 'full':
        generate_full_graph(data)
    else:
        print('Error: Unsupported data kind',kind)
        exit(1)

# sizes (edge count) of link groups in imdb 
imdb_size = {
    'all':29997, 5:8593, 6:5503, 9:5186, 13:3341, 10:2223, 1:1158, 2:1157, 
    8:781, 7:625, 16:278, 12:247, 15:211, 3:207, 11:204, 4:199, 17: 84
}
large_imdb_link_groups = [
    (5, 1), # 9751
    (5, 1, 6), # 15254
    (5, 1, 6, 9), # 20440
    (5, 1, 6, 9, 13, 2), # 24938
    ('all',), #29997
]
medium_imdb_link_groups = [
    (9,), # 5186
    (9, 13), # 8527
    (9, 13, 10), # 10750
    (9, 13, 10, 1, 8), # 12689
    (9, 13, 10, 1, 8, 2, 7), # 14471
]
small_imdb_link_groups = [
    (3, 4), # 406
    (3, 4, 1), # 1564
    (3, 4, 1, 2), # 2721
    (3, 4, 1, 2, 7, 11), # 3550
    (3, 4, 1, 2, 7, 11, 8, 15), # 4542
    (3, 4, 1, 2, 7, 11, 8, 15, 12, 16), # 5067
]

def calc_imdb_size(link_types):
    if link_types:
        return sum([imdb_size(t) for t in link_types])
    else:
        return imdb_size['all']

def descr_link_types(link_types):
    if link_types:
        return '{' + ','.join(map(str,link_types)) + '}'
    else:
        return 'all'

# Prepares IMDB data
def prepare_imdb_data(data):
    link_types = data.get('link_types')
    data['n'] = calc_imdb_size(link_types)
    script = open(load_script_file,'w')
    script.write('--DROP TABLE IF EXISTS MOVIE_LINK;\n')
    script.write('CREATE TABLE MOVIE_LINK(ID INT, MOVIE_ID INT, LINKED_MOVIE_ID INT, LINK_TYPE_ID INT);\n')
    script.write("\copy MOVIE_LINK FROM '%s' CSV\n"%(movie_link_file))
    script.write('DROP TABLE IF EXISTS R;\n');    
    script.write('CREATE TABLE R(A INT, B INT);\n')
    script.write('INSERT INTO R\n')
    script.write('SELECT MOVIE_ID, LINKED_MOVIE_ID FROM MOVIE_LINK')
    if link_types:
        script.write('\nWHERE LINK_TYPE_ID IN %s'%('('+','.join(map(str,link_types))+')',))
    script.write(';\n')
    script.write('DROP TABLE MOVIE_LINK;\n')
    if data.get('indexed'):
        script.write('CREATE INDEX ind1 ON R(A);')
        script.write('CREATE INDEX ind2 ON R(B);')
        script.write('CLUSTER R USING ind1;')
    script.write('ANALYZE;\n')
    script.close()

    print('done')
    sys.stdout.flush()

# Generates data based on Barabási–Albert model 
def generate_barabasi_albert_data(data):
    n = data['n']
    m0 = data['m0']
    try:
        from networkx.generators.random_graphs import barabasi_albert_graph
    except ImportError:
        print('Please install NetworkX package, e.g. `pip install networkx`')

    sys.stdout.flush()
    script = open(load_script_file,'w')
    r = open(r_file,'w')
    script.write('DROP TABLE IF EXISTS R;\n');    
    script.write('CREATE TABLE R(A INT, B INT);\n')
    script.write("COPY R FROM '%s';\n"%(r_file,))
    if data['indexed']:
        script.write('CREATE INDEX ind1 ON R(A);')
        script.write('CREATE INDEX ind2 ON R(B);')
        script.write('CLUSTER R USING ind1;')
    script.write('ANALYZE;\n')
    
    g = barabasi_albert_graph(n,m0)
    m = 0
    for e in g.edges:
        r.write('%s\t%s\n'%(e[0],e[1]))
        m += 1
    data['m'] = m

    r.close()
    script.close()

    print('done')
    sys.stdout.flush()

def generate_full_graph(data):
    n = data['n']

    sys.stdout.flush()
    script = open(load_script_file,'w')
    r = open(r_file,'w')
    script.write('DROP TABLE IF EXISTS R;\n');    
    script.write('CREATE TABLE R(A INT, B INT);\n')
    script.write("COPY R FROM '%s';\n"%(r_file,))
    if data.get('indexed'):
        script.write('CREATE INDEX ind1 ON R(A);')
        script.write('CREATE INDEX ind2 ON R(B);')
        script.write('CLUSTER R USING ind1;')
    script.write('ANALYZE;\n')

    m = 0
    for i in range(n):
        for j in range(n):
            m += 1
            r.write('%s\t%s\n'%(i,j))

    data['m'] = m

    r.close()
    script.close()

    print('done')
    sys.stdout.flush()

PSQL_TIMEOUT_MSG = 'ERROR:  canceling statement due to statement timeout'

#
# Runs Shell postgres command such as psql, createdb, or dropdb. Handles errors
# and unwelcome diagnostic output (NOTICE, WARNING, etc). Potentially breaks on
# FATAL and ERROR.
#
def postgres(command, **params):
    ignore_errors = params.get('ignore_errors',False)
    verbose = params.get('verbose', False)
    msg_on_error = params.get('msg_on_error',None)

    output_f = os.popen(command + ' 2>&1')
    output = output_f.read()
    if PSQL_TIMEOUT_MSG not in output and ('ERROR:' in output or 'FATAL:' in output):
        if not ignore_errors:
            if msg_on_error:
                print(msg_on_error)
            print(output)
            exit(1)
    if verbose or 'WARNING:' in output:
        print(output)

    return output

#
# Loads data into a database. Because this procedure is used in case of a
# timeout of query evaluation, it first forcibly deletes the database thus
# killing any phantom process that may still run. 
#
def load_data(**params):
    quiet = params.get('quiet', False)

    if not quiet:
        print('loading data ', end='... ')
    sys.stdout.flush()
    postgres('dropdb -p ' + str(pg_port) + ' --force --if-exists ' + db_name,
             msg_on_error='Error while attempting to delete database')
    postgres('createdb -p ' + str(pg_port) + ' ' + db_name,
             msg_on_error='Error while attempting to create database')
    postgres('psql -p ' + str(pg_port) + ' -q -f ' + load_script_file + ' ' + db_name,
             msg_on_error='Error while data loading occurred')

    if not quiet:
        print('done')
    sys.stdout.flush()

## 
## QUERIES 
##

#
# The following functions prepare query definition given two parameters: 1)
# length of path, which is equal to number of joins plus 1, and the threshold
# value.  There are 6 functions depending on class of queries and method of
# computing results. The particular choice is indicated in the function name:
# class: TQ1 -> path, TQ2 -> neig[hborhood], TQ3 -> conn[ectivity]
# method: naiv[e], wind[owed]
#

# TQ1 naive
def prep_path_naiv_query(length,threshold):

    query = 'SELECT DISTINCT R0.A, R%i.B\n'%(length-1,)
    query += 'FROM '
    for i in range(length):
        query += 'R AS R%i'%(i,)
        if i < length - 1:
            query += ', '
    query += '\n'
    query += 'WHERE '
    for i in range(length-1):
        query += 'R%i.B = R%i.A'%(i,i+1)
        if i < length-1 - 1:
            query += ' AND '
    query += '\n'
    query += 'LIMIT %i;'%(threshold,)

    return query

# TQ2 naive
def prep_neig_naiv_query(length,threshold):

    query = 'SELECT R0.A\n'
    query += 'FROM '
    for i in range(length):
        query += 'R AS R%i'%(i,)
        if i < length - 1:
            query += ', '
    query += '\n'
    query += 'WHERE '
    for i in range(length-1):
        query += 'R%i.B = R%i.A'%(i,i+1)
        if i < length-1 - 1:
            query += ' AND '
    query += '\n'
    query += 'GROUP BY R0.A\n'
    query += 'HAVING COUNT(DISTINCT R%i.B) >= %i;'%(length-1,threshold)

    return query

# TQ3 naive
def prep_conn_naiv_query(length,threshold):

    query = 'SELECT SUB.X0, SUB.X%i\n'%(length,)
    query += 'FROM (\n'
    query += '   SELECT DISTINCT R0.A AS X0'
    for i in range(length):
        query += ', R%i.B AS X%i'%(i,i+1)
    query += '\n'
    query += '   FROM '
    for i in range(length):
        query += 'R AS R%i'%(i,)
        if i < length - 1:
            query += ', '
    query += '\n'
    query += '   WHERE '
    for i in range(length-1):
        query += 'R%i.B = R%i.A'%(i,i+1)
        if i < length-1 - 1:
            query += ' AND '
    query += '\n'
    query += ') AS SUB \n'
    query += 'GROUP BY SUB.X0, SUB.X%i\n'%(length,)
    query += 'HAVING COUNT(*) >= %i;'%(threshold,)

    return query

# TQ1 windowed
# joining forward; slower on IMDb
def prep_path_wind_query_forward(length,threshold):
    query = 'WITH\n'
    for j in range(1,length+1):
        query += 'J%i AS '%(j,)
        if j == 1:
            query += '(SELECT DISTINCT A AS X0, B AS X%i FROM R),\n'%(j,)
        else:
            query += '(SELECT DISTINCT S%i.X0, R.B AS X%i\n'%(j-1,j)
            query += '       FROM S%i, R\n'%(j-1)
            query += '       WHERE S%i.X%i = R.A),\n'%(j-1,j-1)
        query += 'W%i AS '%(j,)
        query += '(SELECT X0, X%i, ROW_NUMBER() OVER (PARTITION BY X%i) AS RK FROM J%i),\n'%(j,j,j,)
        query += 'S%i AS '%(j,)
        query += '(SELECT X0, X%i FROM W%i WHERE RK <= %i)'%(j,j,threshold,)
        if j < length :
            query += ','
        query +='\n'
    query += 'SELECT X0, X%i FROM S%i LIMIT %i;'%(length,length,threshold)

    return query

# joining backward; faster on IMDb. USE THIS ONE
def prep_path_wind_query_backward(length,threshold):
    query = 'WITH\n'
    for j in reversed(range(1, length + 1)):
        query += 'J%i AS '%(j,)
        if j == length:
            query += '(SELECT DISTINCT A AS X%i, B AS X%i FROM R),\n'%(j-1,j)
        else:
            query += '(SELECT DISTINCT R.A AS X%i, X%i \n'%(j-1,length)
            query += '       FROM R, S%i \n'%(j+1,)
            query += '       WHERE R.B = S%i.X%i),\n'%(j+1,j,)
        query += 'W%i AS '%(j,)
        query += '(SELECT X%i, X%i, ROW_NUMBER() OVER (PARTITION BY X%i) AS RK FROM J%i),\n'%(j-1,length,j-1,j,)
        query += 'S%i AS '%(j,)
        query += '(SELECT X%i, X%i FROM W%i WHERE RK <= %i)'%(j-1,length,j,threshold,)
        if j > 1 :
            query += ','
        query +='\n'
    query += 'SELECT X0, X%i FROM S1 LIMIT %i;'%(length,threshold)

    return query


# TQ2 windowed
# joining forward, slower on IMDb; INCORRECT !!!
def prep_neig_wind_query_forward(length,threshold):
    query = 'WITH\n'
    for j in range(1,length+1):
        query += 'J%i AS '%(j,)
        if j == 1:
            query += '(SELECT DISTINCT A AS X0, B AS X%i FROM R),\n'%(j,)
        else:
            query += '(SELECT DISTINCT S%i.X0, R.B AS X%i\n'%(j-1,j)
            query += '       FROM S%i, R\n'%(j-1)
            query += '       WHERE S%i.X%i = R.A),\n'%(j-1,j-1)
        query += 'W%i AS '%(j,)
        query += '(SELECT X0, X%i, ROW_NUMBER() OVER (PARTITION BY X%i) AS RK FROM J%i),\n'%(j,j,j,)
        query += 'S%i AS '%(j,)
        query += '(SELECT X0, X%i FROM W%i WHERE RK <= %i),\n'%(j,j,threshold,)
    query += 'C AS (SELECT X0, COUNT(*) CNT FROM S%i GROUP BY X0),\n'%(length,)
    query += 'S AS (SELECT X0 FROM C WHERE CNT >= %i)\n'%(threshold,)
    query += 'SELECT X0 FROM S;'

    return query

# joining backward, faster on IMDb; USE THIS ONE
def prep_neig_wind_query_backward(length,threshold):
    query = 'WITH\n'
    for j in reversed(range(1, length + 1)):
        query += 'J%i AS '%(j,)
        if j == length:
            query += '(SELECT DISTINCT A AS X%i, B AS X%i FROM R),\n'%(j-1,j)
        else:
            query += '(SELECT DISTINCT R.A AS X%i, X%i \n'%(j-1,length)
            query += '       FROM R, S%i \n'%(j+1)
            query += '       WHERE R.B = S%i.X%i),\n'%(j+1,j)
        query += 'W%i AS '%(j,)
        query += '(SELECT X%i, X%i, ROW_NUMBER() OVER (PARTITION BY X%i) AS RK FROM J%i),\n'%(j-1,length,j-1,j,)
        query += 'S%i AS '%(j,)
        query += '(SELECT X%i, X%i FROM W%i WHERE RK <= %i),\n'%(j-1,length,j,threshold,)
    query += 'C AS (SELECT X0, COUNT(*) CNT FROM S1 GROUP BY X0),\n'
    query += 'S AS (SELECT X0 FROM C WHERE CNT >= %i)\n'%(threshold,)
    query += 'SELECT X0 FROM S;'

    return query


# TQ3 windowed
# joining forward, faster on IMDb; USE THIS ONE
def prep_conn_wind_query_forward(length,threshold):
    query = 'WITH\n'
    query += 'S1 AS '
    query += '(SELECT DISTINCT A AS X0, B AS X1 FROM R),\n'
    for j in range(2,length+1):
        query += 'J%i AS '%(j,)
        query += '(SELECT DISTINCT '
        query += ''.join(['S%i.X%i, '%(j-1,i) for i in range(j)])
        query += 'R.B AS X%i '%(j,)
        query += '\n'
        query += '       FROM S%i, R\n'%(j-1)
        query += '       WHERE S%i.X%i = R.A),\n'%(j-1,j-1)
        query += 'W%i AS '%(j,)
        query += '(SELECT ' 
        query += ''.join(['X%i, '%(i,) for i in range(j+1)])
        query += 'ROW_NUMBER() OVER (PARTITION BY X0, X%i) AS RK '%(j,) 
        query += 'FROM J%i),\n'%(j)
        query += 'S%i AS '%(j,)
        query += '(SELECT ' 
        query += ', '.join(['X%i'%(i,) for i in range(j+1)]) + ' '
        query += 'FROM W%i WHERE RK <= %i)'%(j,threshold)
        if j < length :
            query += ','
        query +='\n'
    query += 'SELECT X0, X%i FROM S%i GROUP BY X0, X%i HAVING COUNT(*)>=%i'%(
        length,length,length,threshold)
    
    return query


# joining backward, slower on IMDb
def prep_conn_wind_query_backward(length,threshold):
    query = 'WITH\n'
    query += 'S1 AS '
    query += '(SELECT DISTINCT B AS X0, A AS X1 FROM R),\n'
    for j in range(2,length+1):
        query += 'J%i AS '%(j,)
        query += '(SELECT DISTINCT '
        query += ''.join(['S%i.X%i, '%(j-1,i) for i in range(j)])
        query += 'R.A AS X%i '%(j,)
        query += '\n'
        query += '       FROM S%i, R\n'%(j-1)
        query += '       WHERE S%i.X%i = R.B),\n'%(j-1,j-1)
        query += 'W%i AS '%(j,)
        query += '(SELECT ' 
        query += ''.join(['X%i, '%(i,) for i in range(j+1)])
        query += 'ROW_NUMBER() OVER (PARTITION BY X0, X%i) AS RK '%(j,) 
        query += 'FROM J%i),\n'%(j)
        query += 'S%i AS '%(j,)
        query += '(SELECT ' 
        query += ', '.join(['X%i'%(i,) for i in range(j+1)]) + ' '
        query += 'FROM W%i WHERE RK <= %i)'%(j,threshold)
        if j < length :
            query += ','
        query +='\n'
    query += 'SELECT X%i, X0 FROM S%i GROUP BY X0, X%i HAVING COUNT(*)>=%i'%(
        length,length,length,threshold)
    
    return query

query_constructors = {
    'TQ1' : {'naive':prep_path_naiv_query, 'windowed':prep_path_wind_query_backward},
    'TQ2' : {'naive':prep_neig_naiv_query, 'windowed':prep_neig_wind_query_backward},
    'TQ3' : {'naive':prep_conn_naiv_query, 'windowed':prep_conn_wind_query_forward},
}

#
# A succinct description of the query. Used as a column name in the CSV report.
#
def query_descr(query):
    kind    = query['kind']
    method  = query['method']
    k  = query['k']
    threshold   = query['threshold']
    return '%s-%s-K%i'%(kind,method[:4],k)

#
# Prepares a query script file with the appropriate query formulation and timing
# commands. Also, it intentionally disables parallel query evaluation since our
# focus is on single threaded performance.
#
def prepare_query(query, timeout=None):
    kind = query['kind']
    method = query['method']
    length = query['k'] + 1
    threshold = query['threshold']
    query_str = query_constructors[kind][method](length,threshold)
    # print(query_descr(query))
    # print(query_str)
    script = open(query_script_file,'w')
    script.write('SET max_parallel_workers_per_gather = 0;\n')
    if timeout:
        script.write('SET statement_timeout = %d;\n'%(timeout,))
    script.write('\\timing\n')
    script.write(query_str)
    script.close()

#
# Parses the timing output of psql command.
#
def parse_psql_output(s):
    # The output of psql is something like this 
    # ```
    # Timing is on.
    # Time: 1146.152 ms (00:01.146)
    # ```
    # But when timeout occurs, then it can be of this form
    # ```
    # Timing is on.
    # psql:query-script.sql:7: ERROR:  canceling statement due to statement timeout
    # Time: 665.042 ms
    # ```

    if PSQL_TIMEOUT_MSG in s:
        return 'TIMEOUT'
    t = s.split('\n')[-1]
    time_start = s.find('Time: ')+len('Time: ')
    time_end = s.find(' ms',time_start)
    time = s[time_start:time_end]
    return float(time.replace(',', '.'))

#
# Performs a single run of a query in the query script file. Outputs the amount
# of time used or `TIMEOUT` if the query run more than `timeout` seconds. If the
# query reached a timeout, the psql (client) process is killed but the
# corresponding postgres (sever) process is likely to still run. Consequently,
# the database is reloaded which forces deleting the database and killing all
# processes and any lingering cleaning subprocess. 
#
def measure_query_run():
    output = postgres('psql -p '+ str(pg_port) + ' -o /dev/null -f ' + query_script_file + ' ' + db_name)
    time = parse_psql_output(output)

    print('%10s'%(time,),end=' ')
    sys.stdout.flush()

    return time

#
# Runs a query experiment a given number of times. Returns the median value of
# all runs (a timeout is considered to be an infinity).
#
def run_query_experiment(data, query, runs, hard_timeout):

    print('%-.25s'%(query_descr(query),),end=' ')
    sys.stdout.flush()
    results = []
    for i in range(runs):
        results.append(measure_query_run())
        if results[-1] == 'TIMEOUT' and hard_timeout == True:
            break
    results.sort(key=lambda x: x if x != 'TIMEOUT' else float('inf'))
    print('   MEDIAN: %-10s'%(str(results[len(results)//2])))
    sys.stdout.flush()
    return results[len(results)//2]

#
# CSV REPORT 
#
# Result experiments are stored in a report file, which is a CSV file with all
# the relevant informations. Single line of the report file corresponds to
# experiments run on a single data set. The columns contain characteristics of
# the data set (kind, size, m0, etc), setting of the experiment (number of runs,
# timeout threshold), and results on every query from a test suite.
#

#
# The first columns of the report file. This can be any repetition-free sequence
# of 'kind', 'n', 'm0', 'link-types', 'm', 'indexed', 'runs', 'timeout'.
#
_header_base = ('kind','n','m0','m','indexed','runs','timeout')

#
# The remaining columns depend on the queries that are run in an experiment.
#
def make_report_header(queries):
    header = _header_base + tuple(map(query_descr, queries))
    return header 

#
# A line of the CSV report starts with information about data. 
#
def start_report_line(data, timeout, runs):
    line = {'runs':runs, 'timeout': timeout}
    for label in _header_base:
        if label in data:
            line[label] = data[label]
    return line

#
# Runs experiments on given collection of data and given collection of queries.
# It has two mandatory (positional) arguments:
#  `datas`   is a sequence of `data` elements
#  `queries` is a sequence of `query` elements
# It also accepts optional keyword arguments 
#  `name` is a string that will be used for naming the CSV report (.csv
#         extension is added); If not specified, a current time and date will be
#         used. 
#  `runs` is an int indicating number of times the running time of each query is
#         to be measured. If not specified the value `default_runs` shall be
#         used.
#  `timeout` is an int indicating the timeout threshold in microseconds. Query
#         execution will be terminated after this time has elapsed and somewhat
#         costly measures will be taken to make sure there are no lingering
#         processed. 
#  `hard_timeout` is a boolean value, where True that when a timeout occurs, 
#         the next iterations are aborted 

def run_experiments(datas, queries, **params):
    runs    = params.get('runs',default_runs)
    timeout = params.get('timeout',default_timeout)
    hard_timeout = params.get('hard_timeout',default_hard_timeout)
    name    = params.get('name',datetime.datetime.now().strftime('report-%d.%m.%Y-%H-%M-%S'))

    if 'name' in params:
        print('Running test suite:',name)
    if 'timeout' in params:
        print('Timeout set to',timeout,'microseconds')

    results_file = os.path.join(results_dir,name+'.csv')
    
    report_header = make_report_header(queries)
    report_f = open(results_file,'w')
    report = csv.DictWriter(report_f, fieldnames=report_header)
    report.writeheader()

    for data in datas:
        prepare_data(data)
        load_data()
        report_line = start_report_line(data, timeout, runs)
        for query in queries:
            prepare_query(query, timeout)
            result = run_query_experiment(data, query, runs, hard_timeout)
            report_line[query_descr(query)] = result
        report.writerow(report_line)
        report_f.flush()

if __name__ == '__main__':

    # TQ Class 1 (paths) on Synthetic data (Barabási–Albert)
    # TQ Class 2 (neighborhood) on Synthetic data (Barabási–Albert)
    # TQ Class 3 (connectivity) on Synthetic data (Barabási–Albert)
    datas = [ 
        {'kind' : 'ba', 'n' : n, 'm0' : m0, 'indexed' : indexed}
        for m0 in [5,10,15,20,25]
        for n in [32,100,316,1000,3162,10000,31623,100000,316228,1000000]
        for indexed in {False,True}
    ]
    queries =[
        {'kind':kind,'method':method,'k':k,'threshold':10}
        for kind in ['TQ1','TQ2','TQ3']
        for k in [1,2,3,4,5,6,7,8,9,10]
        for method in ['naive','windowed']
    ]
    run_experiments(datas,queries,name='SYNTH.K5-25.M5-25.K1-10',runs=3,timeout=1800000)

    # TQ All classes on Real data (IMDB)
    datas = [ 
        {'kind' : 'imdb', 
         'link-type' : ('all',), 
         'runs' : 3, 
         'indexed' : indexed}
        for indexed in {False,True}
    ] 
    queries = [
        {'kind':kind, 'method':method, 'k':k, 'threshold':10}
        for k in[1,2,3,4,5,6,7,8,9,10]
        for kind in ['TQ1','TQ2','TQ3']
        for method in ['naive','windowed']
    ]
    run_experiments(datas,queries,name='IMDB.K1-10',runs=3,timeout=1800000)
