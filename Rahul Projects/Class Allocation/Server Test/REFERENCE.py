import pandas as pd
import numpy as np
import re
# import regex as re
import  re
import warnings
from datetime import datetime


#google                            3.0.0
#googlesearch-python               1.0.1
#regex                             2022.1.18



def reference_data(sample_df,part_prefix_df,isml_uom):

    sample_df = sample_df
    sample_df.rename(columns={'RECORD_NO':'MATERIAL','LONG_TEXT':'LONG_DESCRIPTION'},inplace=True)
    sample_df["LONG_DESCRIPTION_Original"] = sample_df["LONG_DESCRIPTION"]

    sample_df = sample_df.dropna(subset=["LONG_DESCRIPTION"])

    part_prefix_df = part_prefix_df

    part_prefix_df = part_prefix_df.fillna('')

    part_prefix_df['length'] = part_prefix_df['ORIGINAL_PART_PREFIX'].str.len()

    part_prefix_df.sort_values('length', ascending=False, inplace=True)

    part_prefix_df = part_prefix_df.reset_index(drop = True)

    isml_uom = isml_uom

    def Reference_data_extraction():
        def ref_no_checker(description):
            des = description
            container = []
            manufacture_part_df = part_prefix_df[part_prefix_df["V10_PART_PREFIX"] != "MANUFACTURER"]
            
            for i, row in manufacture_part_df.iterrows():
                cleaned_part_code = re.sub(":", "", row["ORIGINAL_PART_PREFIX"])
                cleaned_part_code = re.sub("\.", "\\.", cleaned_part_code)
                # pat = re.compile(r"(\b%s[:\-\s]{0,9}[a-zA-Z0-9]{1,40}[^,;\s]*)" % cleaned_part_code)
                pat = re.compile(r"(\b[,;\s(]?%s[.\s:\-\s]{0,9}[a-zA-Z0-9]{1,40}[^,;\s)]*)" % cleaned_part_code)
                try:
                    match = re.findall(pat, des)
                    if match:
                        #below line for removing finded item
                        des = re.sub(re.escape(str(re.search(pat,des).group().strip())), '', des)
                    else:
                        continue
                    for i in range(len(match)):
                        replace_lst = match[i] + "~" + row["V10_PART_PREFIX"]
                        if replace_lst not in container:
                            container.append(replace_lst)
                except TypeError:
                    continue
            if container:
                return container
            else:
                return None

        sample_df_5 = sample_df.dropna(subset=["LONG_DESCRIPTION"])

        sample_df_5["Captured_part_no_list"] = sample_df_5["LONG_DESCRIPTION"].apply(lambda x: ref_no_checker(x))

        sample_df_5 = sample_df_5.dropna(subset=["Captured_part_no_list"])

        sample_df_5_explode = sample_df_5.assign(PartNumber=sample_df_5.Captured_part_no_list).explode(
            'PartNumber')

        sample_df_5_explode.drop(columns=["Captured_part_no_list"], inplace=True)

        sample_df_5_explode["Reference_Type"] = sample_df_5_explode["PartNumber"].apply(lambda x: x.split("~")[1])

        sample_df_5_explode["Reference_Number"] = sample_df_5_explode["PartNumber"].apply(lambda x: x.split("~")[0])

        sample_df_5_output = sample_df_5_explode[sample_df_5_explode.duplicated("Reference_Type", keep=False)]

        sample_df_5_output = sample_df_5_output.drop(columns=["PartNumber"])

        sample_df_5_output = sample_df_5_output.sort_values(by=["Reference_Number"])

        return sample_df_5_output

    ref = Reference_data_extraction()

    ref1 = ref.copy()

    ref1 = ref1.reset_index(drop = True)

    ref1["Reference_Number"] = ref1["Reference_Number"].apply(lambda part_numb :re.sub(r'\)','',part_numb))

    ref1["Reference_Number"] = ref1["Reference_Number"].apply(lambda part_numb :re.sub(r'\(','',part_numb))

    #removing prefix from the Reference_Number column
    def prefix_remove(x):      
        source = x
        for i in (range(part_prefix_df.shape[0])):
            des = re.sub(part_prefix_df['ORIGINAL_PART_PREFIX'][i],'',source)
            source = des
        return source
    ref['Reference_Number'] = ref['Reference_Number'].apply(lambda x:prefix_remove(x))

    #cleaning unwanted data
    ref["Reference_Number"] = ref["Reference_Number"].apply(lambda part_numb :re.sub(r'^[\sa-zA-Z]*$','',part_numb))

    ref["Reference_Number"] = ref["Reference_Number"].apply(lambda part_numb :re.sub(r'^[:\s,.!@#$%^\-&*a-zA-Z;:\s,.!@#$%^\-&*]*$','',part_numb))

    ref["Reference_Number"] = ref["Reference_Number"].apply(lambda part_numb :re.sub(r'^\W+$','',part_numb))

    ref = ref[ref.Reference_Number != '  ']

    ref = ref[ref.Reference_Number != ' ']

    ref = ref[ref.Reference_Number != '']

    ref = ref.dropna()

    ref["Reference_Number"] = ref["Reference_Number"].apply(lambda part_numb :re.sub(r'\)','',part_numb))

    ref["Reference_Number"] = ref["Reference_Number"].apply(lambda part_numb :re.sub(r'\(','',part_numb))

    ref = ref.reset_index(drop = True)

    #removing partnumber column from long_description
    def partnumber_remove(x):      
        source = x
        for i in (range(ref1.shape[0])):
            pat = re.compile(r"(\b{}\b)".format(ref1['Reference_Number'][i]))
            des = re.sub(pat,'',source)
            source = des
        return source
    ref['LONG_DESCRIPTION'] = ref['LONG_DESCRIPTION'].apply(lambda x:partnumber_remove(x))

    ref = ref.fillna('')

    #cleaning
    lst = ['MANUFACTURER','NO','MANUFACTURER NO','MANUFACTURERNO','EQUIPMENT','SUPPLIER','ALTERNATE','TAG','SERIAL NO']

    for i in lst:
        ref["Reference_Number"] = ref.Reference_Number.str.replace(r'{}'.format(i), '')
    REF1 = ref.copy()

    ref.drop(columns=["Reference_Number"], inplace=True)

    # print("step1 completed")

    #Extracting numarics from descriptions

    def numarics_extraction(description):
        numarics_list1 = []
        # patteren = re.findall(r"\b([,\s;]?[\W][0-9]*[^,\s;A-Za-z])\b",description)
        patteren = re.findall(r"\b([,\s;]?[\W][0-9]{4,40}[^,\s;A-Za-z])\b",description)

        for i in range(len(patteren)):
            replace_lst = patteren[i]+"~"+ "REFERENCE NUMBER"
            if replace_lst not in numarics_list1:
                numarics_list1.append(replace_lst)
        return numarics_list1

    ref["numarics"] = ref["LONG_DESCRIPTION"].apply(lambda x: numarics_extraction(x))

    ref = ref.dropna(subset=["numarics"])

    ref = ref.assign(Numarics_data=ref.numarics).explode('Numarics_data')

    ref.drop(columns=["numarics"], inplace=True)

    ref.fillna('', inplace = True)

    ref = ref[ref['Numarics_data'] != '']

    ref = ref.reset_index(drop = True)

    ref["Reference_Type"] = ref["Numarics_data"].apply(lambda x: x.split("~")[1])

    ref["Reference_Number"] = ref["Numarics_data"].apply(lambda x: x.split("~")[0])

    ref.drop(columns=["Numarics_data"], inplace=True)

    #for removing less than 3 letters

    ref["Reference_Number"] = ref.Reference_Number.str.replace(r'\b([\w\W]{1,3})\b', '')

    #removing numarics from long_descriptions

    ref["Reference_Number"] = ref["Reference_Number"].apply(lambda part_numb :re.sub(r'\)','',part_numb))

    ref["Reference_Number"] = ref["Reference_Number"].apply(lambda part_numb :re.sub(r'\(','',part_numb))

    for i in range(ref.shape[0]):
        ref["LONG_DESCRIPTION"] = ref["LONG_DESCRIPTION"].apply(lambda part_numb :re.sub('\b{}\b'.format(ref["Reference_Number"][i]),'',part_numb))

    REF2 = ref.copy()

    ref.drop(columns=["Reference_Number"], inplace=True)

    # print("step2 completed")

    #Extracting alpha_numarics_Referencedata

    def alpha_numarics(description):
        numarics_list = []
        patteren = re.findall(r"\b([,;\s]{1,4}[0-9]{0,10}[-/]{0,2}[a-zA-Z]{0,20}[0-9]{1,20}[a-zA-Z]{1,20}[,/;\s:-]{0,10})\b",description)

        for i in range(len(patteren)):
            replace_lst = patteren[i]+"~"+ "REFERENCE NUMBER"
            if replace_lst not in numarics_list:
                numarics_list.append(replace_lst)
        return numarics_list

    ref["alpha_num_lst"] = ref["LONG_DESCRIPTION"].apply(lambda x: alpha_numarics(x))

    ref = ref.dropna(subset=["alpha_num_lst"])

    ref2 = ref.assign(Reference_AlphaNumarics_data=ref.alpha_num_lst).explode('Reference_AlphaNumarics_data')

    ref.drop(columns=["alpha_num_lst"], inplace=True)

    ref2.fillna('', inplace = True)

    ref2 = ref2[ref2['Reference_AlphaNumarics_data'] != '']

    ref2 = ref2.reset_index(drop = True)

    ref2["Reference_Type"] = ref2["Reference_AlphaNumarics_data"].apply(lambda x: x.split("~")[1])

    ref2["Reference_Number"] = ref2["Reference_AlphaNumarics_data"].apply(lambda x: x.split("~")[0])

    ref2.drop(columns=["Reference_AlphaNumarics_data","alpha_num_lst"], inplace=True)

    #for removing less than 3 letters

    ref2["Reference_Number"] = ref2.Reference_Number.str.replace(r'\b([\w\W]{1,3})\b', '')

    for i in range(ref2.shape[0]):
        ref["LONG_DESCRIPTION"] = ref["LONG_DESCRIPTION"].apply(lambda part_numb :re.sub('\b{}\b'.format(ref2["Reference_Number"][i]),'',part_numb))

    #removing specialcharecters from Reference_AlphaNumarics_data

    ref2["Reference_Number"] = ref2.Reference_Number.str.replace(r'[\W]', '')

    REF3 = ref2.copy()

    def alpha_numarics_1(description):
        numarics_list_1 = []
        patteren = re.findall(r"\b([,\s;]{1,4}[a-zA-Z]{0,20}[-]{0,2}[0-9]{0,10}[a-zA-Z]{1,20}[0-9]{1,20}[,;\s]{0,10})\b",description)
        # patteren = re.findall(r"\b([,\s;]{1,4}[a-zA-Z]{0,20}[-]{0,2}[0-9]{0,10}[a-zA-Z]{1,20}[0-9]{1,20}[,/;\s]{0,10}[a-zA-Z0-9]{0,10})\b",description)

        for i in range(len(patteren)):
            replace_lst = patteren[i]+"~"+ "REFERENCE NUMBER"
            if replace_lst not in numarics_list_1:
                numarics_list_1.append(replace_lst)
        return numarics_list_1

    ref["alpha_num_lst_1"] = ref["LONG_DESCRIPTION"].apply(lambda x: alpha_numarics_1(x))

    ref = ref.dropna(subset=["alpha_num_lst_1"])

    ref3 = ref.assign(Reference_AlphaNumarics_data1=ref.alpha_num_lst_1).explode('Reference_AlphaNumarics_data1')

    ref.drop(columns=["alpha_num_lst_1"], inplace=True)

    ref3.fillna('', inplace = True)

    ref3 = ref3[ref3['Reference_AlphaNumarics_data1'] != '']

    ref3 = ref3.reset_index(drop = True)

    ref3["Reference_Type"] = ref3["Reference_AlphaNumarics_data1"].apply(lambda x: x.split("~")[1])

    ref3["Reference_Number"] = ref3["Reference_AlphaNumarics_data1"].apply(lambda x: x.split("~")[0])

    ref3.drop(columns=["alpha_num_lst_1","Reference_AlphaNumarics_data1"], inplace=True)

    # #for removing less than 3 letters

    # ref3["Reference_Number"] = ref3.Reference_Number.str.replace(r'\b([\w\W]{1,4})\b', '')

    #removing specialcharecters from Reference_AlphaNumarics_data1

    # ref3["Reference_Number"] = ref3.Reference_Number.str.replace(r'[\W]', '')

    ref3["Reference_Number"] = ref3.Reference_Number.str.replace(r'\b([0-9]{1,20})\b', '')

    #removing duplicates

    ref3 = ref3.drop_duplicates(subset=["Reference_Number"])

    ref3.fillna('', inplace = True)

    ref3 = ref3[ref3['Reference_Number'] != '']

    ref3 = ref3.reset_index(drop = True)

    REF4 = ref3.copy()

    # droping columns from the all df's

    REF1.drop(columns=["LONG_DESCRIPTION"], inplace=True)

    REF1 = REF1.reset_index(drop = True)

    REF2.drop(columns=["LONG_DESCRIPTION"], inplace=True)

    REF2 = REF2.reset_index(drop = True)

    REF3.drop(columns=["LONG_DESCRIPTION"], inplace=True)

    REF3 = REF3.reset_index(drop = True)

    REF4.drop(columns=["LONG_DESCRIPTION"], inplace=True)

    REF4 = REF4.reset_index(drop = True)

    REF2.rename(columns={'Reference_Numarics':'Reference_Number'},inplace=True)

    REF3.rename(columns={'Reference_AlphaNumarics_data':'Reference_Number'},inplace=True)

    REF4.rename(columns={'Reference_AlphaNumarics_data1':'Reference_Number'},inplace=True)

    #concating all dataframes one by one

    frames = [REF1, REF2, REF3, REF4]

    df_combined = pd.concat(frames)

    df_combined = df_combined.reset_index(drop = True)

    #sorting material column

    df_combined['MATERIAL'] = df_combined['MATERIAL'].astype(str)

    df_combined = df_combined.sort_values(by=["MATERIAL"])

    #Removing special charecters
    search_list = [';',':',',','-',' ','\.']
    for i in search_list:
        df_combined["Reference_Number"] = df_combined.Reference_Number.str.replace(r'^{}'.format(i), '')

    search_list = [';',':',',','-',' ','\.']
    for i in search_list:
        df_combined["Reference_Number"] = df_combined.Reference_Number.str.replace(r'^{}'.format(i), '')

    #removing uom's from the Reference_Number column

    uom_list = isml_uom["NEW_UOM"].tolist()

    for u_list_val in uom_list:
        indx_lst = df_combined.loc[df_combined['Reference_Number'].str.endswith("""{}""".format(u_list_val))]['Reference_Number'].index.tolist()
        df_combined.loc[indx_lst, 'Reference_Number'] = ''

    #removing null rows from the df_combined

    df_combined.fillna('', inplace = True)

    df_combined = df_combined[df_combined['Reference_Number'] != '']

    df_combined = df_combined.reset_index(drop = True)
    df_combined.rename(columns={'Reference_Type':'Reference_Type_data','Reference_Number':'Reference_Number_data'},inplace=True)
    # print(df_combined)
    return df_combined

