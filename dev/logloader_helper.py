import pandas as pd
import io
import gzip
import re


#this helper is usefull when some lines get shifted due to a bad parsing
#when loading a log with read_csv
class log_read_csv():
    def __init__(self,path,sep = r'\s(?=(?:[^"]*"[^"]*")*[^"]*$)(?![^\[]*\])'):
        self.wrong_lines = {}
        self.sep = sep
        self.cleaned = False
        self.fix_suggest = None
        try:
            self.df=pd.read_csv(path,
                 sep=r'\s(?=(?:[^"]*"[^"]*")*[^"]*$)(?![^\[]*\])', 
                 engine='python', na_values=['-'], header=None,
                 usecols=[0, 3, 4, 5, 6, 7, 8,10],
                 names=['ip', 'time', 'request', 'status', 'size', 'referer',
                 'user_agent','req_time'],
                 converters={'status': int, 'size': int, 'req_time': int})
            self.cleaned = True
            print("this log seems clean", 
                   "you can get it with retrieve_dataframe.")
        except (ValueError, TypeError) :
            self.df=pd.read_csv(path,
                 sep=r'\s(?=(?:[^"]*"[^"]*")*[^"]*$)(?![^\[]*\])', 
                 engine='python', na_values=['-'], header=None,
                 usecols=[0, 3, 4, 5, 6, 7, 8,10],
                 names=['ip', 'time', 'request', 'status', 'size', 'referer',
                 'user_agent','req_time'])
            print("This log couldn't be loaded properly. Use the method "
                  ".analyse_lines to have a look at what went wrong\n")
            print("gathering wrong lines...")
            file = gzip.open(path)
            lines = file.readlines()
            #an index column is added, so that a row can "know" its index 
            df_index = self.df.reset_index()
            df_index['status']= df_index.apply(lambda row :
                self.convert_int_collect_failures(row,'status',lines), 
                                               axis=1)
            df_index.pop('index')
            file.close()
            self.df = df_index
            print("Done")

    def convert_int_collect_failures(self,row,col,lines):
        #function to be used with .apply
        try:
            ans = int(row[col])
        except (ValueError, TypeError):
            self.wrong_lines[row['index']] = str(lines[row['index']])[1:-3]
            ans= pd.np.nan
        return ans

    def field_recovery(self,group):
    #usage: Given a group from a splited line, 
    #makes a first guess of the field it belongs to.
    #This first guess will be used by the function generate_fix_suggest.
    ################################################################
#   REGEX PATTERNS OF FIELDS              
        field_patterns={'ip': r'.*\d{2,3}\.\d{2,3}\.\d{2,3}\.\d{2,3}',
        'time' : r'\[.*\]', 
        'request' : r'\"[A-Z]{3,}.*\/.*\"',# a request starts with a HTTP method
        'status' : r'\d{3}(?!\d)', #'200', '204', '404' etc
        'number' : r'\d+', #corresponds to 'size' or 'req_time'
        'referer' : r'(\"\-\")|\"http.*', #if provided, we assume that the 
                                          #protocol is http or https
        'user_agent' : r'(\".*\")|(.*\.\w{2,3})', #assumes the user_agent
                        #is given either as an url or as a string
        'empty' : r'\-',
        'unknown' : r'.*' #this is the fallback of our switch
        }
    ###############################################################
    #we now proced to a switch using the dictionary above :
        for k in field_patterns.keys():
            if re.match(string=group,pattern=field_patterns[k]):
                return k

    def split_wrong_lines(self):
        return [re.split(self.sep,self.wrong_lines[k]) for k 
                in self.wrong_lines.keys()]

    def analyse_lines(self, index = 0):
    #This method generates a suggested fix_pattern to be used with
    #.recover_wrong_lines. It also displays an example of splited wrong line
    #together with this fix_pattern so that the user can gain some insight.

    #index is meant to be the index of the list split. Hence,
    #index=0 corresponds to the first wrong line.
    #If needed, one can eg use .analyse_lines(9) to generate
    #the fix_suggest with the 10th wrong line.
        if self.cleaned:
            print("Nothing to analyse, dataframe is clean")
        else:
            split=self.split_wrong_lines()
            if split is not None:
                self.generate_fix_suggest(index,split)
                print("line split :\n")
                print(split[index])
                print("\n")
                print("suggested fields :\n")
                print(self.fix_suggest)

    def generate_fix_suggest(self,index,split):
        raw_fix_suggest = [self.field_recovery(group) for group in 
                            split[index]]      
        self.fix_suggest = []
        met_sofar = []
        for field in raw_fix_suggest :
            if field == 'status' and 'status' in met_sofar:
                self.fix_suggest.append('number')
            elif (field not in ['empty','ip','unknown','number'] and field in met_sofar):
                self.fix_suggest.append('unknown')
            else:
                self.fix_suggest.append(field)
                met_sofar.append(field)

    def line_to_dic(self, line, fix_pattern):
    #used by recover_wrong_lines, since dataframe.append requires a dictionary
    #whose keys are the named fields. 

    #The field 'unknown' is ignored.
    #Since (in our example logs) the field 'size' is often empty, we treat
    #a late 'empty' field as a 'size' field and convert the corresponding
    #content to NaN if it is indeed empty.
        ret={name : '' for name in ['ip', 'time', 'request', 'status', 'size',
                                    'referer','user_agent','req_time']}
        groups = re.split(string=line,pattern=self.sep)
        if len(groups) != len(fix_pattern):
            error_message =("fix_pattern is incompatible with line, " 
                            "expected {0} groups, got {1}").format(len(groups),
                                                              len(fix_pattern))
            raise ValueError(error_message)
        else:
            for i in range(len(groups)):
                if fix_pattern[i]=='number' or fix_pattern[i]=='empty':
                    if i > 8:
                        col = 'req_time'
                    else:
                        col = 'size'
                    if groups[i] == '-':
                        ret[col] = pd.np.nan
                    else:
                        ret[col] = groups[i]
                else:
                    if fix_pattern[i] in ret.keys():
                        ret[fix_pattern[i]] += groups[i]
        return ret        

    def recover_wrong_lines(self, fix_pattern = None):
        if fix_pattern is None:
            if self.fix_suggest is None:
                print("no fix_pattern is loaded, use .analyse_lines to\n"
                      "generate one")
                return
            else:
                fix_pattern = self.fix_suggest
            #First, remove unclean lines
        for k in self.wrong_lines.keys():
            self.df = self.df.drop(k)
        #gather the fields of each unclean line into a clean row
        for k in self.wrong_lines.keys():
            self.df = self.df.append(self.line_to_dic(self.wrong_lines[k],
                                                      fix_pattern),
                                                      ignore_index=True)
        self.cleaned = True
        self.wrong_lines.clear()

    def retrieve_dataframe(self):
        if self.cleaned:
            return self.df
        else:
            print("Returning unclean dataframe, run .analyse_lines then \n"
                  "use .recover_wrong_lines to clean the dataframe")
            return self.df
