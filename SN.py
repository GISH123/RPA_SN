import fitz
import tabula
import re
import pandas as pd
import glob

def search_index(all_df):
    whole_df_index = []
    for i in range(len(all_df)):
        c_df = all_df[i]

        # 表一：自動出場以及下限價觀察頻率
        if(c_df.columns[1] == "觀察期間開始日(含)"):
            whole_df_index.append(i)

        # 表二：觀察期間開始日, 觀察期間結束日與配息交割日表
        elif(c_df.columns[0] == "本商品"):
            whole_df_index.append(i)

        # 本產品線有可能會有表三：自動提前出場價格表 (比如說 W170 FINAL TS.pdf)
        elif(c_df.columns[1] == "自動提前出場價"):
            whole_df_index.append(i)

        # 表三及之後：期初訂價, 自動出場觸發水準, 下限價, 轉換價及配息下層界線
        elif(c_df.columns[1] == "連結標的"):
            whole_df_index.append(i)

        #  W080 最後一張表比較特殊
        elif(c_df.columns[0] == "費用項目"):
            whole_df_index.append(i)
    return whole_df_index
    
def merge_part_df(all_df, whole_df_index):
    '''
    修正被頁數分割的pdf，並且把他們合併起來
    '''
    # 把被頁數分割的df合併起來
    whole_df_list = []
    c_whole_df = pd.DataFrame()
    first_col_name = all_df[0].columns

    for i in range(len(all_df)):
        c_df = all_df[i].copy()

        # 其實 需求的話 做到前面幾個df就能斷了，但反正目前還是可以執行下去，就先存到很後面的df吧
        if(i==12):
            break

        if i in whole_df_index:
            whole_df_list.append(c_whole_df.reset_index(drop=True))
            # 本次讀到的c_df為表頭
            c_whole_df = c_df
            # 將標頭儲存起來，備用
            first_col_name = c_df.columns

        #第一次抓取df不用執行連結表格的動作
        elif(i==0):
            c_whole_df = c_df

        else:
            # 把標頭弄進row裡面去，因為只是被截斷的df，不會有標頭，標頭的那些原本應該是資料的一部
            c_df.index += 1
            c_df.loc[0] = c_df.columns
            # 將c_df的columns變為第一個c_df的column name
            c_df.columns = first_col_name
    #         print(i)
    #         print(first_col_name)
            c_df = c_df.sort_index()

            c_whole_df = c_whole_df.append(c_df)
            
    return whole_df_list
    
def check_list(whole_df, col_name, col_index = 0):
    # 某一些表的欄位太少，會導致index out of bound
    if(len(whole_df.columns)<=col_index):
        return False
    # 有抓取到的情況
    elif(whole_df.columns[col_index] == col_name):
        return True
    else:
        return False
        
def classify_tables(whole_df_list):
    '''將合併出來的表(whole_df)分門別類，並取名稱'''
    table_dict = {}
    price_get = False

    for i in whole_df_list:
        if(check_list(i, '自動出場觀察頻率', col_index = 0)):
            table_dict['觀察頻率表'] = i
            print('有抓取到[觀察頻率表]')
        elif(check_list(i, '觀察期間開始日(含)', col_index = 1)):
            table_dict['配息交割日表'] = i
            print('有抓取到[配息交割日表]')
        elif(check_list(i, '自動提前出場價', col_index = 1)):
            table_dict['自動提前出場價格表'] = i
            print('有抓取到[自動提前出場價格表]')
        elif(check_list(i, '本商品', col_index = 0)):
            table_dict['到期給付表'] = i
            print('有抓取到[到期給付表]')

        # 目前先暫訂只抓第一個期初訂價表
        elif(check_list(i, '期初訂價', col_index = 2) and not price_get):
            table_dict['期初訂價表'] = i
            print('有抓取到[期初訂價表]')
            price_get = True

        elif(check_list(i, '彭博代碼', col_index = 2)):
            table_dict['彭博代碼表'] = i
            print('有抓取到[彭博代碼表]')
            
    return table_dict
    
def observe_table_df(table_dict, product_name):
    observe_date = pd.DataFrame()
    # TODO: 將PDF檔名刻上去
    if('配息交割日表' in table_dict):
        observe_date[product_name] = table_dict['配息交割日表']['觀察期間結束日(含)']
    else:
        print("沒有讀取到[配息交割日表]")
    return observe_date
    
    
def search_text(input_string, table_dict):
    
    def re_helper(pattern, input_string, group_index):
        m = re.search(pattern,input_string)
        if m:
            return m.group(group_index)
        else:
            print('找不到' + pattern)
    
    text_dict = {}
    
    pattern = r'國際證劵編碼ISIN：[ @]+([\w*-]+)'
    text_dict['ISIN'] = re_helper(pattern, input_string, 1)
    
    pattern = r'瑞士信貸倫敦分行發行(\d+)'
    text_dict['期間(M)'] = re_helper(pattern, input_string, 1)
    
    pattern = r'商品計價幣別：[@](\w+).\((\w+)\)'
    text_dict['幣別'] = re_helper(pattern, input_string, 2)    
    
    # 特殊抓取邏輯，就不用function了
    m = re.search(r'11. 主要給付項目及其計算方式：@(\w+)', input_string)
    if m:
        #邏輯： 
        # 每月配息	DRA
        # 每月固定配息	FCN
        if m.group(1) == "主要給付項目為每月配息":
            text_dict['架構'] = 'DRA'
        elif m.group(1) == "主要給付項目為每月固定配息":
            text_dict['架構'] = 'FCN'
        elif m.group(1) == "主要給付項目為自動提前出場給付":
            text_dict['架構'] = 'STEPDOWN'
        else:
            text_dict['架構'] = '本次沒抓到符合的文字，請查看文件是否有出現符合規格的情況'

    # [特別注意] 這邊的× 是叉叉符號 不是英文字母x= =
    pattern = r'× 100% × (\d+\.\d+)'
    text_dict['配息率/紅利率'] = re_helper(pattern, input_string, 1) + '%'

    # W170不是這樣敘述 列TODO
    pattern = r'記憶式自動提前出場條款：自(\w+)'
    text_dict['觀察起日/下次觀察日'] = re_helper(pattern, input_string, 1)     
    
    # 有 自動提前出場價格表 的情況
    if('自動提前出場價格表' in table_dict):
        # 找出表格 第一欄位 自動提前出場價 的百分比部分
        text_dict['提前出場%'] = re.search('(\d+.\d+)',table_dict['自動提前出場價格表']['自動提前出場價'][0]).group(1) + '%'
        
    else:
        pattern = r'觸發水準為期初訂價之(\d+)'
        text_dict['提前出場%'] = re_helper(pattern, input_string, 1) + '%'
    
    pattern = r'其轉換價為期初訂價之(\d+.\d+)'
    text_dict['執行/轉換價%'] = re_helper(pattern, input_string, 1) + '%'

    pattern = r'其下限價為期初訂價之(\d+.\d+)'
    text_dict['KI%'] = re_helper(pattern, input_string, 1) + '%'

    pattern = r'交易日：[@](\w+)'
    text_dict['交易日(期初比價日)'] = re_helper(pattern, input_string, 1)   

    pattern = r'發行日：[@](\w+)'
    text_dict['發行日'] = re_helper(pattern, input_string, 1)

    pattern = r'期末評價@日：@(\w+)'
    text_dict['期末評價日'] = re_helper(pattern, input_string, 1)
    
    pattern = r'到期日：[@](\w+)'
    text_dict['到期日'] = re_helper(pattern, input_string, 1)

    return text_dict
    
def freq_table_df(text_dict, table_dict):
    # 表一：自動出場以及下限價觀察頻率
    if('觀察頻率表' in table_dict):
        freq_df = table_dict['觀察頻率表']
    else:
        print("沒有讀取到[觀察頻率表]")

    if(freq_df['自動出場觀察頻率'][0]=='每日'):
        text_dict['KO機制'] = 'DAILY'

    elif(freq_df['自動出場觀察頻率'][0]=='每月'):
        text_dict['KO機制'] = 'PERIOD END'

    # ki機制 
    if("下限價觀察頻率" not in freq_df):
        text_dict['KI機制'] = None
    elif(freq_df['下限價觀察頻率'][0]=='每日'):
        text_dict['KI機制'] = 'AKI'
    elif(freq_df['下限價觀察頻率'][0]=='每月'):
        text_dict['KI機制'] = 'PKI'
    elif(freq_df['下限價觀察頻率'][0]=='到期觀察'):
        text_dict['KI機制'] = 'EKI'
        
    return text_dict
    
def one_pdf_process(filepath):
    text = []
    with fitz.open(filepath) as doc:
        pages = doc.pageCount
        for i in range(pages):
            text.append(doc.get_page_text(i)) 

    # 使用tabula讀入所有pdf裡面的表格
    all_df = tabula.read_pdf(filepath, pages='all', lattice = True)
    name = re.search(r'\\(\w+)',filepath)
    # 產品名稱，即檔名的第一個空格前的部分
    product_name = name.group(1)

    whole_df_index = search_index(all_df)
    whole_df_list = merge_part_df(all_df, whole_df_index)

    table_dict = classify_tables(whole_df_list)

    observe_date = observe_table_df(table_dict, product_name)
    # 轉成需求的規格
    observe_date = observe_date.T.reset_index().rename(columns={'index': '代號'})
    
    text_dict = {}
    input_string = '@'.join(text).replace('\n','@')

    text_dict = search_text(input_string, table_dict)
    text_dict = freq_table_df(text_dict, table_dict)
    
    # 如果有這行文字，則在KO機制後面加 "MEMORY"字串
    
    # !!!!TODO : 這部分還有待跟業管確認，不是所有PDF文字都如下,MEMORY 判定有問題
    m = re.search(r'都曾經大於或等於其各自之自動提前出場價格', input_string)
    if m:
        text_dict['KO機制'] += ' MEMORY'

    # 表二：觀察期間開始日, 觀察期間結束日與配息交割日表
    observe_df = table_dict['配息交割日表']
    # 判斷P4.(A)[KO觀察起日/下次觀察日]於P4(D)[觀察期間結束日(含)]的表格中為第幾個觀察日
    text_dict['Non-Call'] = str(observe_df.loc[observe_df['觀察期間結束日(含)'] == text_dict['觀察起日/下次觀察日']].index[0] + 1) + 'M'
    
    # 表三：期初訂價, 自動出場觸發水準, 下限價, 轉換價及配息下層界線
    result_df = pd.DataFrame()
    # 取非中文字部分
    result_df['進場價'] = table_dict['期初訂價表']['期初訂價'].str.extract('([^\u4E00-\u9FFF]+)')[0]

    # 12.(算表4?) 連結標的資產，及其相對權重、與投資績效之關連情形：
    # 以空格做分割 取第一個元素
    result_df['連結標的'] = table_dict['彭博代碼表']['彭博代碼'].str.split().str.get(0)

    # 另一個連結標的= =
    result_df['連結標的(Bloomberg)'] = table_dict['彭博代碼表']['彭博代碼'] + " Equity"

    # 將df更新之前弄好的text_dict
    for k, v in text_dict.items():
        result_df[k] = v

    # 將PDF名稱刻上去
    result_df['產品代碼'] = product_name
    result_df['Product Code'] = product_name
    result_df['Issuer'] = 'CS'
    
    # 有"自動出場價格表的情況" 需額外增加新欄位
    if '自動提前出場價格表' in table_dict:

        # 主要給付項目為每月固定配息，且文件中有自動提前出場價格表 則架構  = Stepdown FCN
        result_df.loc[result_df['架構']=='FCN', '架構'] = 'Stepdown FCN'

        # 每期降階幅度
        price_1 = re.search('(\d+\.\d+)',table_dict['自動提前出場價格表']['自動提前出場價'][0]).group(1)
        price_1 = float(price_1)
        price_2 = re.search('(\d+\.\d+)',table_dict['自動提前出場價格表']['自動提前出場價'][1]).group(1)
        price_2 = float(price_2)
        result_df['每期降階幅度'] = str(price_1 - price_2) + '%'

        #降階起始門檻 = 提前出場%
        result_df['降階起始門檻'] = result_df['提前出場%']
        
        # 欄位順序調動
        result_df = result_df[['產品代碼', '架構', 'KO機制', 'KI機制', 'Non-Call', '幣別', \
                               '連結標的', '進場價', '觀察起日/下次觀察日', '每期降階幅度', '降階起始門檻', '提前出場%', \
                               '執行/轉換價%', 'KI%', '配息率/紅利率', '期間(M)', '交易日(期初比價日)', '發行日', '期末評價日', '到期日', \
                               'Issuer', 'ISIN', '連結標的(Bloomberg)', 'Product Code'
                              ]]
    
    else:
        # 欄位順序調動
        result_df = result_df[['產品代碼', '架構', 'KO機制', 'KI機制', 'Non-Call', '幣別', \
                               '連結標的', '進場價', '觀察起日/下次觀察日','提前出場%', \
                               '執行/轉換價%', 'KI%', '配息率/紅利率', '期間(M)', '交易日(期初比價日)', '發行日', '期末評價日', '到期日', \
                               'Issuer', 'ISIN', '連結標的(Bloomberg)', 'Product Code'
                              ]]
    
    return result_df, observe_date
    
if __name__ == "__main__":
    # Using '*' pattern 
    filepath_list = []
    result_df_total = pd.DataFrame()
    observe_date_total = pd.DataFrame()

    for name in glob.glob('./pdf/*'):
        filepath_list.append(name)
        

    for i in range(len(filepath_list)):
        try:
            filepath = filepath_list[i]
            result_df, observe_date = one_pdf_process(filepath)

            result_df_total = result_df_total.append(result_df)
            observe_date_total = observe_date_total.append(observe_date)
        except:
            print(filepath + "<- 檔案有問題")
            continue
            
    # result_df_total.to_excel
    # observe_date_total.to_excel