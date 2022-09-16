import pandas as pd
import numpy as np
import re
import regex as re
import warnings
from datetime import datetime


warnings.filterwarnings("ignore")


def reference_data(sample_df,part_prefix_df,isml_uom):
    now = datetime.now()
    print('start time', now)

    sample_df = sample_df
    print(sample_df)
    # print('='*100, '\n', sample_df, '\n', '='*100)

    # try:
    #     sample_df.drop(columns=["SHORT_TEXT"], inplace=True)
    # except:
    #     pass

    sample_df.rename(columns={'REGISTERED_RECORD_NO':'MATERIAL','LONG_TEXT':'LONG_DESCRIPTION'},inplace=True)
    # sample_df.rename(columns={'REGISTERED_RECORD_NO': 'MATERIAL', 'FFT': 'LONG_DESCRIPTION'}, inplace=True)


    # sample_df.rename(columns={'RECORD_NO': 'MATERIAL', 'LONG_TEXT': 'LONG_DESCRIPTION'}, inplace=True)


    sample_df["LONG_DESCRIPTION_Original"] = sample_df["LONG_DESCRIPTION"]

    # sample_df["LONG_DESCRIPTION_Original"] = sample_df["LONG_DESCRIPTION"]

    sample_df = sample_df.dropna(subset=["LONG_DESCRIPTION"])


    part_prefix_df = part_prefix_df.fillna('')

    part_prefix_df['length'] = part_prefix_df['ORIGINAL_PART_PREFIX'].str.len()

    part_prefix_df.sort_values('length', ascending=False, inplace=True)

    part_prefix_df = part_prefix_df.reset_index(drop=True)


    def Reference_data_extraction():
        def ref_no_checker(description):
            des = description
            container = []
            container2 = []
            manufacture_part_df = part_prefix_df
            for i, row in manufacture_part_df.iterrows():
                cleaned_part_code = re.sub(":", "", row["ORIGINAL_PART_PREFIX"])
                cleaned_part_code = re.sub("\.", "\\.", cleaned_part_code)
                # pat = re.compile(r"(\b[,;\s(]?%s[.\s:\-\s]{0,9}[a-zA-Z0-9]{1,40}[^,;\s)]*)" % cleaned_part_code)
                pat = re.compile(r"(\b[:#.\s]?%s[.\s:\-\s]{0,9}[a-zA-Z0-9]{1,40}[^,;\s]*)" % cleaned_part_code)
                # print(pat,"111111111111111")
                try:
                    match = re.findall(pat, des)
                    container2 += match
                    if match:
                        # below line for removing finded item
                        des = re.sub(re.escape(str(re.search(pat, des).group().strip())), '', des)
                    else:
                        continue
                    for i in range(len(match)):
                        replace_lst = match[i] + "~" + row["V10_PART_PREFIX"]
                        if replace_lst not in container:
                            container.append(replace_lst)
                except TypeError:
                    continue

            return container,container2


        sample_df_5 = sample_df.dropna(subset=["LONG_DESCRIPTION"])

        sample_df_5["Captured_part_no_list"] = sample_df_5["LONG_DESCRIPTION"].apply(lambda x: ref_no_checker(x)[0])
        # print(sample_df_5["Captured_part_no_list"])

        sample_df_5["Captured_part_no_list2"] = sample_df_5["LONG_DESCRIPTION"].apply(lambda x: ref_no_checker(x)[1])
        # print(sample_df_5["Captured_part_no_list2"])

        sample_df_5 = sample_df_5.dropna(subset=["Captured_part_no_list"])
        # print(sample_df_5,"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")

        sample_df_5_explode = sample_df_5.assign(PartNumber=sample_df_5.Captured_part_no_list).explode(
            'PartNumber')

        sample_df_5_explode = sample_df_5_explode.dropna()

        sample_df_5_explode.drop(columns=["Captured_part_no_list"], inplace=True)

        sample_df_5_explode["Reference_Type"] = sample_df_5_explode["PartNumber"].apply(lambda x: x.split("~")[1])

        # print(sample_df_5_explode["Reference_Type"],"99999999999999999")

        sample_df_5_explode["Reference_Number"] = sample_df_5_explode["PartNumber"].apply(lambda x: x.split("~")[0])

        # print(sample_df_5_explode["Reference_Number"],"@@@@@@@@@@@@@@@@")

        sample_df_5_output = sample_df_5_explode[sample_df_5_explode.duplicated("Reference_Type", keep=False)]

        sample_df_5_output = sample_df_5_output.drop(columns=["PartNumber"])

        sample_df_5_output = sample_df_5_output.sort_values(by=["Reference_Number"])

        return sample_df_5_output

    ref = Reference_data_extraction()
    # print(ref,"yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy")

    # removing prefix from the Reference_Number column

    def prefix_remove(x):
        source = x
        for i in (range(part_prefix_df.shape[0])):
            try:
                des = re.sub(part_prefix_df['ORIGINAL_PART_PREFIX'][i], '', source)
                source = des
            except:
                continue
        return source

    ref['Reference_Number'] = ref['Reference_Number'].apply(lambda x: prefix_remove(x))

    # cleaning unwanted data
    ref["Reference_Number"] = ref["Reference_Number"].apply(lambda part_numb: re.sub(r'^[\sa-zA-Z]*$', '', part_numb))

    ref["Reference_Number"] = ref["Reference_Number"].apply(
        lambda part_numb: re.sub(r'^[:\s,.!@#$%^\-&*a-zA-Z;:\s,.!@#$%^\-&*]*$', '', part_numb))

    ref["Reference_Number"] = ref["Reference_Number"].apply(lambda part_numb: re.sub(r'^\W+$', '', part_numb))

    ref = ref[ref.Reference_Number != '  ']

    ref = ref[ref.Reference_Number != ' ']

    ref = ref[ref.Reference_Number != '']

    ref = ref.dropna()

    ref = ref.reset_index(drop=True)


    # cleaning
    lst = ['MANUFACTURER', 'NO', 'MANUFACTURER NO', 'MANUFACTURERNO', 'EQUIPMENT', 'SUPPLIER', 'ALTERNATE', 'TAG',
           'SERIAL NO', "DRG", "NUMBER", "NUM"]

    for i in lst:
        ref["Reference_Number"] = ref.Reference_Number.str.replace(r'{}'.format(i), '')


    #Removing reference_number column values from the description
    cnt=0
    for sp,i in zip(ref.LONG_DESCRIPTION,ref.Captured_part_no_list2):
        hold1=sp
        for j in i:
            hold1=hold1.replace(j,"")
        ref["LONG_DESCRIPTION"][cnt]=hold1
        cnt+=1

    REF1 = ref.copy()

    ref.drop(columns=["Reference_Number","Captured_part_no_list2"], inplace=True)

    ref = ref.drop_duplicates(subset=["MATERIAL"])

    first = datetime.now()
    print('stage one time', first)


    # Extracting alpha_numarics_Referencedata

    def alpha_numarics(description):
        numarics_list = []
        numarics_list1 = []
        patteren = re.findall(
            r"\b([,;\s]{1,4}[0-9]{0,10}[-/]{0,2}[a-zA-Z]{0,20}[0-9]{1,20}[a-zA-Z]{1,20}[.,;\s]{0,10})\b", description)

        numarics_list1 += patteren

        for i in range(len(patteren)):
            replace_lst = patteren[i] + "~" + "REFERENCE NUMBER"
            if replace_lst not in numarics_list:
                numarics_list.append(replace_lst)
        return numarics_list,numarics_list1


    ref["alpha_num_lst"] = ref["LONG_DESCRIPTION"].apply(lambda x: alpha_numarics(x)[0])

    ref["alpha_num_lst1"] = ref["LONG_DESCRIPTION"].apply(lambda x: alpha_numarics(x)[1])

    ref = ref.dropna(subset=["alpha_num_lst"])

    ref2 = ref.assign(Reference_AlphaNumarics_data=ref.alpha_num_lst).explode('Reference_AlphaNumarics_data')

    ref.drop(columns=["alpha_num_lst"], inplace=True)

    ref2.fillna('', inplace=True)

    ref2 = ref2[ref2['Reference_AlphaNumarics_data'] != '']

    ref2 = ref2.reset_index(drop=True)

    ref2["Reference_Type"] = ref2["Reference_AlphaNumarics_data"].apply(lambda x: x.split("~")[1])

    ref2["Reference_Number"] = ref2["Reference_AlphaNumarics_data"].apply(lambda x: x.split("~")[0])

    # print(ref2["Reference_Number"])

    ref2.drop(columns=["Reference_AlphaNumarics_data", "alpha_num_lst"], inplace=True)

    #  removing Reference_Number column data from LONG_DESCRIPTION

    cnt1=0
    for val,i in zip(ref2.LONG_DESCRIPTION,ref2.alpha_num_lst1):
        hold2 = val
        for j in i:
            hold2 = hold2.replace(j,"")
        ref2["LONG_DESCRIPTION"][cnt1]=hold2
        cnt1+=1


    # for removing less than 3 letters

    ref2["Reference_Number"] = ref2.Reference_Number.str.replace(r'\b([\w\W]{1,3})\b', '')

    # removing uom's from the Reference_Number column

    uom_list = isml_uom["NEW_UOM"].tolist()

    for u_list_val in uom_list:
        indx_lst = ref2.loc[ref2['Reference_Number'].str.endswith("""{}""".format(u_list_val))][
            'Reference_Number'].index.tolist()
        ref2.loc[indx_lst, 'Reference_Number'] = ''

    third = datetime.now()
    print('stage two', third)

    REF2 = ref2.copy()

    ref2 = ref2.drop_duplicates(subset=["MATERIAL"])


    def alpha_numarics_1(description):
        numarics_list_1 = []
        numarics_list_2 = []
        for i, row in ref.iterrows():
            try:
                patteren1 = re.findall(
                    r"\b([,\s;]{1,4}[a-zA-Z]{0,20}[-]{0,2}[0-9]{0,10}[a-zA-Z]{1,20}[0-9]{1,20}[.,;\s]{0,10})\b", description)
                numarics_list_2 += patteren1
                for i in range(len(patteren1)):
                    replace_lst = patteren1[i] + "~" + "REFERENCE NUMBER"
                    # print(replace_lst)
                    if replace_lst not in numarics_list_1:
                        numarics_list_1.append(replace_lst)
            except TypeError:
                continue

        return numarics_list_1,numarics_list_2


    ref2["alpha_num_lst_1"] = ref2["LONG_DESCRIPTION"].apply(lambda x: alpha_numarics_1(x)[0])

    ref2["alpha_num_lst_2"] = ref2["LONG_DESCRIPTION"].apply(lambda x: alpha_numarics_1(x)[1])

    ref2 = ref2.dropna(subset=["alpha_num_lst_1"])

    ref3 = ref2.assign(Reference_AlphaNumarics_data1=ref2.alpha_num_lst_1).explode('Reference_AlphaNumarics_data1')

    ref2.drop(columns=["alpha_num_lst_1"], inplace=True)

    ref3.fillna('', inplace=True)

    ref3 = ref3[ref3['Reference_AlphaNumarics_data1'] != '']

    ref3 = ref3.reset_index(drop=True)

    ref3["Reference_Type"] = ref3["Reference_AlphaNumarics_data1"].apply(lambda x: x.split("~")[1])

    ref3["Reference_Number"] = ref3["Reference_AlphaNumarics_data1"].apply(lambda x: x.split("~")[0])

    ref3.drop(columns=["alpha_num_lst_1", "Reference_AlphaNumarics_data1"], inplace=True)

    ref3["Reference_Number"] = ref3.Reference_Number.str.replace(r'\b([0-9]{1,20})\b', '')

    # removing extracted alphanumarics from longdescription

    cnt3=0
    for val3,i in zip(ref3.LONG_DESCRIPTION,ref3.alpha_num_lst_2):
        hold3 = val3
        for j in i:
            hold3 = hold3.replace(j,"")
        ref3["LONG_DESCRIPTION"][cnt3] = hold3
        cnt3+=1

    # removing uom's from the Reference_Number column
    uom_list = isml_uom["NEW_UOM"].tolist()

    for u_list_val in uom_list:
        indx_lst = ref3.loc[ref3['Reference_Number'].str.endswith("""{}""".format(u_list_val))][
            'Reference_Number'].index.tolist()
        ref3.loc[indx_lst, 'Reference_Number'] = ''

    # removing duplicates

    ref3 = ref3.drop_duplicates(subset=["Reference_Number"])

    ref3.fillna('', inplace=True)

    ref3 = ref3[ref3['Reference_Number'] != '']

    ref3 = ref3.reset_index(drop=True)

    REF3 = ref3.copy()

    three = datetime.now()
    print('stage three', three)
    # Extracting numarics from descriptions

    def numarics_extraction(description):
        numarics_list1 = []
        patteren = re.findall(r"\b([,\s;]?[0-9]{4,40}[\W]{0,40}[0-9]{0,40}[^,\s;A-Za-z])\b", description)
        for i in range(len(patteren)):
            replace_lst = patteren[i] + "~" + "REFERENCE NUMBER"
            if replace_lst not in numarics_list1:
                numarics_list1.append(replace_lst)
        return numarics_list1


    ref3["numarics"] = ref3["LONG_DESCRIPTION"].apply(lambda x: numarics_extraction(x))

    ref3 = ref3.dropna(subset=["numarics"])

    ref4 = ref3.assign(Numarics_data=ref3.numarics).explode('Numarics_data')

    ref3.drop(columns=["numarics"], inplace=True)

    ref4.fillna('', inplace=True)

    ref4 = ref4[ref4['Numarics_data'] != '']

    ref4 = ref4.reset_index(drop=True)

    ref4["Reference_Type"] = ref4["Numarics_data"].apply(lambda x: x.split("~")[1])

    ref4["Reference_Number"] = ref4["Numarics_data"].apply(lambda x: x.split("~")[0])

    ref4.drop(columns=["Numarics_data", "numarics"], inplace=True)

    # for removing less than 3 letters

    ref4["Reference_Number"] = ref4.Reference_Number.str.replace(r'\b([\w\W]{1,3})\b', '')

    REF4 = ref4.copy()

    ref4.drop(columns=["Reference_Number"], inplace=True)

    ref4 = ref.drop_duplicates(subset=["MATERIAL"])


    # concating all dataframes one by one

    frames = [REF1, REF2, REF3, REF4]

    df_combined = pd.concat(frames)

    df_combined = df_combined.reset_index(drop=True)

    df_combined.drop(columns=["Captured_part_no_list2","alpha_num_lst1","alpha_num_lst_2"], inplace=True)


    # sorting material column

    df_combined['MATERIAL'] = df_combined['MATERIAL'].astype(str)

    df_combined = df_combined.sort_values(by=["MATERIAL"])

    # Removing special charecters
    search_list = [';', ':', ',', '-', ' ', '\.']
    for i in search_list:
        df_combined["Reference_Number"] = df_combined.Reference_Number.str.replace(r'^{}'.format(i), '')

    search_list = [';', ':', ',', '-', ' ', '\.']
    for i in search_list:
        df_combined["Reference_Number"] = df_combined.Reference_Number.str.replace(r'^{}'.format(i), '')

    # Removing all special charecters from the reference_number column for STRIP_REFERENCE_NO column
    df_combined["STRIP_REFERENCE_NO"] = df_combined.Reference_Number.str.replace(r'[\W]', '')


    # removing null rows from the df_combined

    df_combined.fillna('', inplace=True)

    df_combined = df_combined[df_combined['Reference_Number'] != '']

    df_combined = df_combined.reset_index(drop=True)

    vendor_list = ["MANUFACTURER", "BRAND NAME"]

    df_combined['VENDOR_NAME'] = ''

    for i in (range(df_combined.shape[0])):
        if df_combined['Reference_Type'][i] in vendor_list:
            df_combined['VENDOR_NAME'][i] = df_combined['Reference_Number'][i]
            df_combined['Reference_Number'][i] = ''
            df_combined['STRIP_REFERENCE_NO'][i] = ''
        else:
            continue

    four = datetime.now()
    print('stage four', four)
    # ----------Adding Columns----------------------------
    df_combined['REGION'] = 'IN'
    df_combined['LOCALE'] = 'en_US'
    df_combined['VENDOR_ID'] = 'UNKNOWN'
    df_combined['R_STXT_FLAG'] = 'Y'
    df_combined['R_LTXT_FLAG'] = 'Y'
    df_combined['CREATE_BY'] = 'DXP'
    df_combined['EDIT_BY'] = 'DXP'
    # df_combined['VENDOR_NAME'] = 'UNKNOWN'

    print(df_combined)

    # ----------------------------------------------------
    df_combined.to_excel('ref_excel.xlsx', index=False)

    return df_combined