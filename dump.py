import locale

for col in rd.columns:
    print(col + '\t : ' + str(type(rd[col][1]).__name__) + '\t' + str(len(rd[col].unique()))+ ' levels \t' + str(rd[col].unique()[:10]) + '\n stats: \n' + str(rd[col].describe()))   

rd.dtypes

#locale.setlocale(locale.LC_ALL, 'english_US')
datetime.strptime("12/12/2015", "%x")

for key in locale.locale_alias.keys():
  try:
    locale.setlocale(locale.LC_ALL, 'english_US')
    for i in rd['Date']:
      i = datetime.strptime(i,'%x')
  except ValueError:
        pass
        
datetime.strptime("1245/12/15", "%Y/%m/%d")



#------------- search for date and time --------------
month_names = ['january', 'february', 'march', 'april' , 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december', 'jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'sept', 'oct', 'nov', 'dec']
days = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday']
valid = ['/' , ':' , '-', '.', ' ', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
ampm = ['am', 'pm']

# check if its in date format
def is_date(dates):
  pat=[]
  # setting max string length for date
  for date in dates:
    if len(date) > 40:
      return (False, False, seps)
      
    # if it has month name / day 
    for month in month_names: 
      if (month in date) or (month.title() in date):
        # write code to get separators
        return (True, False, pat)
    for day in days: 
      if (day in date) or (day.title() in date):
        # write code to get separators
        return (True, False, pat)
        
    # checking for valid characters
    for l in date:
      if l not in valid:
        return (False, False, pat)
      elif l.isdigit():
        pat.append('d')
      else:
        pat.append(l)       
  return (False, True, pat)
  
  
def get_date_format(dates, is_num, seps):
  date_format=''
  for date in dates:
    if len(

row = datetime.strptime(row,date_format)

for col in rd.columns:
  if type(rd[col]) is str:
    samp = random.sample(list(rd[col]), 50)
    is_num, is_lit, seps = is_date(samp)
    if is_num or is_lit:
      get_date_format(samp, is_num, seps)

#--------------- get column statistics -------------------

def get_col_stats(rd):
  for col in rd.columns:
    print(col + '\t : ' + str(type(rd[col][1]).__name__) + '\t' + str(len(rd[col].unique()))+ ' levels \t' + str(rd[col].unique()[:10]) + '\n stats: \n' + str(rd[col].describe()))   

header=0
date_cols=[0]
sep=','
    

cat = subprocess.Popen(["hadoop", "fs", "-cat", "/path/to/myfile"], stdout=subprocess.PIPE)
for line in iter(cat.stdout.readline, ''): 
    print line,   # include the comma
    
    
data_rs = data_sp.randomSplit()

data_rs = data_sp.takeSample(False,len(data_sp)/10)
data_rs = map(list, zip(*data_rs))