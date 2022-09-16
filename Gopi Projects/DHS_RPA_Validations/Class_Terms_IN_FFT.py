import pandas as pd
from fastapi import FastAPI
import uvicorn
from fastapi import Form
import re
from tqdm import tqdm
import cx_Oracle
from typing import Optional


def CLASS_FFT_VALIDATION(fft_text):
    ref_text = fft_text
    ref_text.fillna('', axis=1, inplace=True)
    ref_text['CLASS_TERM_IN_FFT'] = ''

    for ind, string in enumerate(tqdm(ref_text['CLASS_TERM'])):
        if string == '' or string == 'UNKNOWN' or len(string) == 1:
            pass
        else:
            words = []
            string_1 = re.sub(r'[\(\)+$^*\\.?;:\[\]\{\}=]', '', string)
            descr = re.sub(r'[\(\)+$^*\\.?;:\[\]\{\}=]', '', ref_text['MASTER_COLUMN10'][ind])

            inf_values = re.findall(r'\b{}\b'.format(string_1.strip()), descr)

            [words.append(i) for i in inf_values if i not in words]
            if len(words) == 1:
                ref_text.loc[ref_text.index[ind]:ref_text.index[ind], 'CLASS_TERM_IN_FFT'] = ','.join(words)
            else:
                pass

    ref_text = ref_text.loc[ref_text['CLASS_TERM_IN_FFT'] != '']
    # ref_text.to_excel('test.xlsx',index=False)

    return ref_text
# CLASS_FFT_VALIDATION()
