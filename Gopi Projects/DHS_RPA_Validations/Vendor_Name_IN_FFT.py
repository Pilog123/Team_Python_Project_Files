import pandas as pd
import re
from tqdm import tqdm
import cx_Oracle


def VENDOR_FFT_VALIDATION(fft_text, ref):
    ref.columns = ['RECORD_NO', 'REFERENCE_TYPE', 'REFERENCE_NO', 'VENDOR_NAME', 'TABLE_NAME']
    ref_text = fft_text.merge(ref, on='RECORD_NO', how='right')
    ref_text.fillna('', axis=1, inplace=True)

    ref_text['VENDOR_NAME_IN_FFT'] = ''
    for ind, string in enumerate(tqdm(ref_text['VENDOR_NAME'])):
        if string == '' or string == 'UNKNOWN' or len(string) == 1:
            pass
        else:
            words = []
            string_1 = re.sub(r'[\(\)+$^*\\.?;:\[\]\{\}=]', '', string)
            descr = re.sub(r'[\(\)+$^*\\.?;:\[\]\{\}=]', '', ref_text['MASTER_COLUMN10'][ind])

            inf_values = re.findall(r'\b{}\b'.format(string_1.strip()), descr)

            [words.append(i) for i in inf_values if i not in words]
            if len(words) == 1:
                ref_text.loc[ref_text.index[ind]:ref_text.index[ind], 'VENDOR_NAME_IN_FFT'] = ','.join(words)
            else:
                pass

    ref_text = ref_text.loc[ref_text['VENDOR_NAME_IN_FFT'] != '']
    # ref_text.loc[:,['RECORD_NO','MASTER_COLUMN10','VENDOR_NAME','FFT_Duplicates_Vendor']].to_excel('test.xlsx',index=False)
    return ref_text

# VENDOR_FFT_VALIDATION()
