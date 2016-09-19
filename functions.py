#------------- determines datatype of input -----------------

def get_dtype(s):
    try:
        int(s)
        return IntegerType()
    except ValueError:
        try:
            float(s)
            return FloatType()
        except ValueError:
            try:
                parse(s)
                return TimestampType()
            except ValueError:
                return StringType()

                
#----- iterate through given list by random sampling and try to determine dtype ----------
def get_type(list):
    for f in list:
        