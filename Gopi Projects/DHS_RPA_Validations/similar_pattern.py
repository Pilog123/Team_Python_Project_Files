import pandas as pd
import cx_Oracle
import string
import re 

# text = '1.0619.1-DGO-65F-0.45-09-81605.3-1'


def generate_pattern(text):
    # print(text)
    alphabets = list(string.ascii_lowercase+string.ascii_uppercase)
    numbers = list(range(10))
    symbols = [':', "'", '\\', '@', '{', '%', '-', ',', '&', '<', '`', '}', '.', '_', '–',"-","=",']', '!', '>', ';', '#', '$', '/',' ']
    spl_symobls = ['~', '+', '[', '^', '(', '"', '*', '|', '?', ')']
    text1=text
    pattern = ''
    while len(text1)>0:
        # print(text1)
        count_text=0
        for i in text1:
            if i in alphabets:
                text1 = re.sub(f'^{i}','',text1)
                count_text += 1
            else:
                break            
        if count_text>=1:
            pattern += '[A-Za-z]{}'.format({count_text})

        number_count = 0
        for j in text1:
            if j.isnumeric():
                text1 = re.sub(f'^{j}','',text1)
                
                number_count += 1
            else:
                break 
        if number_count>=1:
            pattern += '[0-9]{}'.format({number_count})        

        special_count = 0
        for k in text1:
            # print(k)
            if str(k) in symbols:
                # print(k)
                text1 = re.sub(f'^{k}','',text1)
                pattern += k
            elif k in spl_symobls:
                text1 = re.sub(f'^\{k}','',text1)

                pattern += '\{}'.format(k)            
            else:
                break
    
    # print(pattern)
    return pattern
    #     print(number_count,count_text,special_count)    
    # print(pattern)
    # print(re.search('{}'.format(pattern),'1.0619.1-DFO-65F-0.49-09-91605.3-2'))
# generate_pattern('4HG-72-1959')
# def regex_pattern_generator(a1):
#     s1=''
#     for i,j in enumerate(a1.split()):
#         if j.isnumeric():
#             s1+=f"[0-9]{ {len(j)} }"
#         else:
#             s1+=f"[a-zA-Z]{ { len(j) } }"
#     k1=r"^"+s1+"$"
#     return k1
# a1="!rapam ved 321"
# d1=regex_pattern_generator(a1)
# if re.search(d1, re.sub(r'\s+', '', "!param dev 123")):
#     print("The pattern of given string is matching",d1)