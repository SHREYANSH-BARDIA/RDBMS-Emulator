import re
import pandas as pd
import sys
import numpy as np
import os
import csv

def add_table(tokens):
    table_metadata = dict()
    table_name = tokens[2]
    table_metadata[table_name] = None
    columns = []
    i=0
    attributes = tokens[3:]
    columns = [i for i in attributes]
    # print('columns:',columns)
    df = pd.DataFrame(columns = columns)
    df.to_csv('D:/PERSONAL/Projects/Database Emulator/data/{}.csv'.format(table_name),index=False)
            
            
            
def delete_rows(table_name,iterator,filter_attr,filter_value):
    
    try:
        x = int(table_name[-1])
        table_name = table_name[:-1]
        
        
    except:
        table_name+='1'
    
    path = 'D:/PERSONAL/Projects/Database Emulator/data/{}.csv'.format(table_name)
    
    for df in iterator:
        i=0
        df.reset_index(inplace=True,drop=True)
        rows_to_drop = []
        while i < len(df):
            if(df.iloc[i][filter_attr]==filter_value):
                rows_to_drop.append(i)
                i+=1
                
            else:
                i+=1
                
        df.drop(rows_to_drop,axis=0,inplace=True)
        df.reset_index(inplace=True,drop=True)
                
        if(os.path.isfile(path)):
            # with open(path,'a') as f
            df.to_csv(path,mode='a',index=False,header=False)
                    
        else:
            df.to_csv(path,index=False)      
                    
                    
    print('rows deleted. Your new table name is:',table_name)
                

def delete_table(tokens):
    
    table_name = tokens[2]
    os.remove('D:/PERSONAL/Projects/Database Emulator/data/{}.csv'.format(table_name))
    
    
def set_value(table_name,iterator,attr_to_modify,value,filter_attr=False,filter_value=False):
    
    new_df = pd.DataFrame()
    
    try:
        x = int(table_name[-1])
        table_name = table_name[:-1]
        
        
    except:
        table_name+='1'
    
    path = 'D:/PERSONAL/Projects/Database Emulator/data/{}.csv'.format(table_name)
    
    if(filter_attr):
    
        for df in iterator:
            df.loc[df[filter_attr] ==filter_value,attr_to_modify] = value
            
            if(os.path.isfile(path)):
                df.to_csv(path,mode='a',index=False,header=False)
                        
            else:
                df.to_csv(path,index=False)
            new_df = pd.concat([new_df,df])
                    
    
    else:
        
        for df in iterator:
            df[attr_to_modify] = value
            new_df = pd.concat([new_df,df])
        

        
    new_df.to_csv('D:/PERSONAL/Projects/Database Emulator/data/{}.csv'.format(table_name),index=False)
    
    print('Table update completed. Your new table name is:',table_name)
                    
    
    
def update_table(tokens):
    
    is_filter = 0
    table_name = tokens[1]
    iterator = pd.read_csv('D:/PERSONAL/Projects/Database Emulator/data/{}.csv'.format(table_name),chunksize=1000)

    
    if(tokens[2]=='SET'):
        attr_to_modify = tokens[3]
        value = tokens[5][1:-1]
    
        if('FILTER' in tokens):
            is_filter = tokens.index('FILTER')
            filter_attr = tokens[is_filter+1]
            filter_value = tokens[is_filter+3][1:-1]
            if(tokens[2]=='SET'):
                set_value(table_name,iterator,attr_to_modify,value,filter_attr,filter_value)
                
        else:
            set_value(table_name,iterator,attr_to_modify,value)
            
    elif(tokens[2]=='DELETE'):
        
        filter_attr = tokens[4]
        filter_value = tokens[6][1:-1]  
        
        delete_rows(table_name,iterator,filter_attr,filter_value)
    
    
    
        
def check_duplicates(key,table_name):
    
    generator = pd.read_csv('D:/PERSONAL/Projects/Database Emulator/data/{}.csv'.format(table_name),chunksize=1000)
    
    try:
        while True:
            x = next(generator)
            primary_key = x.iloc[:,0]
            key = int(key[1:-1])
            if(key in primary_key):
                return "Duplicate primary key detected"
            
    except StopIteration:
        return "Unique key detected"
        
        
    
    

def check_rows(tokens):
    
    len_records = len(tokens)
    index = 5
    message = check_duplicates(tokens[5],tokens[2])
    # print('message:',message)
    if(message=="Unique key detected"):
        while(index<len_records):
            record,index = insert_rows(tokens[index:],index)
    
            with open('D:/PERSONAL/Projects/Database Emulator/data/{}.csv'.format(tokens[2]),'a') as f:
                f.write(record)
                f.write('\n')
    
            if(index<len_records):
                index+=1
                
    else:
        print(message)
        



def insert_rows(tokens,index):
    
    
    record = ''
    i=0
    while(tokens[i]!=']'):
        if(tokens[i]=='['):
            index+=1
            i+=1
            continue
        record+=tokens[i][1:-1]+','
        index+=1
        i+=1
    
    record = record[:-1]
    return record,index

def ops_prep(ops_to_check,aliases):
    
    left_table = ops_to_check['left_table']
    left_column = ops_to_check['left_column']
    right_table = ops_to_check['right_table']
    if(ops_to_check['right_column']):
        right_column = ops_to_check['right_column']
        r_operand = right_table[right_column]

    else:
        r_operand=right_table

    operator = ops_to_check['operation']

    new_data = False    
    try:
        r_operand = int(r_operand)
        
    except:
        pass
    
    
    new_data = check_operation(left_table, left_column, r_operand, operator)
    return new_data
    
def gte(left_table,left_column,r_operand):
    new_data = left_table[left_table[left_column]>=r_operand]
    return new_data

def gt(left_table,left_column,r_operand):
    new_data = left_table[left_table[left_column]>r_operand]
    return new_data

def lte(left_table,left_column,r_operand):
    new_data = left_table[left_table[left_column]<=r_operand]
    return new_data

def lt(left_table,left_column,r_operand):
    new_data = left_table[left_table[left_column]>=r_operand]
    return new_data

def eq(left_table,left_column,r_operand):

    new_data = left_table[left_table[left_column]==r_operand]
    return new_data
    

def within(left_table,left_column,r_operand):
    
    new_data2 = pd.DataFrame()

    for value in r_operand:
        if(value[0]=="'"):
            row = left_table[left_table[left_column]==value[1:-1]]

        new_data2 = pd.concat([new_data2,row])

    return new_data2
    
def amidst(left_table,left_column,r_operand):
    new_data2 = pd.DataFrame()
    start = int(r_operand[0][1])
    end = int(r_operand[1][1])
    indices_to_drop = []

    # new_data2 = left_table[left_table[left_column].between(start,end)]
    
    for i in range(left_table.shape[0]):
        if(int(left_table[left_column].iloc[i])<start or int(left_table[left_column].iloc[i])>end):
            indices_to_drop.append(i)
            
    left_table.drop(indices_to_drop,axis=0,inplace=True)
    # left_table.reset_index(drop=True,inplace=True)
    # print('new chunk shape:',left_table.shape)
            
    return left_table
    # return new_data2


def check_operation(left_table,left_column,r_operand,operator):
    # print('operator:',operator)
    
    if(operator=='='):
        new_data = eq(left_table,left_column,r_operand)
        return new_data
    
    if(operator=='>'):
        new_data = gt(left_table,left_column,r_operand)
        return new_data
    
    if(operator=='<'):
        new_data = lt(left_table,left_column,r_operand)
    
    if(operator=='>='):
        new_data = gte(left_table,left_column,r_operand)
        return new_data
    
    if(operator=='<='):
        new_data = lte(left_table,left_column,r_operand)
        return new_data

    if(operator=='WITHIN'):
        new_data = within(left_table,left_column,r_operand)
        return new_data
                
    if(operator=='AMIDST'):
        new_data = amidst(left_table,left_column,r_operand)
        return new_data    


def check_or(ops_to_check,aliases):
    
    new_data = ops_prep(ops_to_check[0],aliases)


    if(new_data.shape[0]==0):
        new_data2 = ops_prep(ops_to_check[1],aliases)
        
        return new_data2
            
    else:
        return new_data
            
            
def check_and(ops_to_check,aliases):
    
    new_data = ops_prep(ops_to_check[0],aliases)
            
    if(not new_data.shape[0]==0):
        left_table = new_data
        new_data2 = ops_prep(ops_to_check[1],aliases)
        return new_data2
            
    else:
        return new_data


def check_ops(tokens,functions,filter_operators):

    for i in functions:
        try:
            functions[i] = tokens.index(i)
        except ValueError:
            pass

    for i in filter_operators:
        try:
            filter_operators[i] = tokens.index(i)
        except ValueError:
            pass
    return functions,filter_operators

def get_outer_loop(table_name,columns):
    
    if(list(columns)[0]!='all'):
        outer = pd.read_csv('D:/PERSONAL/Projects/Database Emulator/data/{}.csv'.format(table_name),usecols=columns,chunksize=1000)    
    else:
        outer = pd.read_csv('D:/PERSONAL/Projects/Database Emulator/data/{}.csv'.format(table_name),chunksize=1000)
    return outer

def get_inner_loop(table_name,columns):
    if(list(columns)[0]!='all'):
        inner = pd.read_csv('D:/PERSONAL/Projects/Database Emulator/data/{}.csv'.format(table_name),usecols=columns,chunksize=1000)    
    else:
        inner = pd.read_csv('D:/PERSONAL/Projects/Database Emulator/data/{}.csv'.format(table_name),chunksize=1000)
    return inner
    

def iter_outer_loop(outer):
    
    for i in outer:
        yield i
        
def iter_inner_loop(inner):
    
    for i in inner:
        yield i
    
def get_table_attrs(table_attrs,lookup_attrs,comparisons,join,stack_attr,aliases,all_aggregators,ordered,tokens,keywords,functions,filter_operators,join_keywords,aggr_functions,ordering_functions,have_comparisons):
    data = dict()
    
    data_list = []
    
    table_names = []

    for i in table_attrs:
        if(list(table_attrs[i])[0]!='all'):

            df = pd.read_csv('D:/PERSONAL/Projects/Database Emulator/data/{}.csv'.format(i),usecols=table_attrs[i],chunksize=1000)
            data_list.append(df)
            table_names.append(i)
            
        else:
            df = pd.read_csv('D:/PERSONAL/Projects/Database Emulator/data/{}.csv'.format(i),chunksize=1000)
            data_list.append(df)
            table_names.append(i)
            
    final_data = pd.DataFrame()
    if(len(table_names)>1):
        left_table_size = 0
        right_table_size = 0
        size = 0
        
        if(len(data_list)>1):
            for i in data_list[0]:
                left_table_size+=1
            
            for i in data_list[1]:
                right_table_size+=1
                
        if(left_table_size<=right_table_size):
            outer_table_name = table_names[0]
            outer_table_columns = table_attrs[outer_table_name]
            inner_table_name = table_names[1]
            inner_table_columns = table_attrs[inner_table_name]
            
        else:
            outer_table_name = table_names[1]
            outer_table_columns = table_attrs[outer_table_name]
            inner_table_name = table_names[0]
            inner_table_columns = table_attrs[inner_table_name]
            
        outer = get_outer_loop(outer_table_name, outer_table_columns)
        inner = get_inner_loop(inner_table_name,inner_table_columns)
        
        outer_iter = iter_outer_loop(outer)
        
        k = 0
        
        while True:
            try:
                j = 0
                outer_table = next(outer_iter)
                # print('chunk memory usage:',outer_table.info(memory_usage='deep'))
                data[outer_table_name] = outer_table
                inner = get_inner_loop(inner_table_name,inner_table_columns)
                inner_iter = iter_inner_loop(inner)
                while True:
                    try:
                        inner_table = next(inner_iter)
                        # print('chunk memory usage:',inner_table.info(memory_usage='deep'))
                        data[inner_table_name] = inner_table
                        
                        
                        new_data = filterout(aggr_functions,have_comparisons,filter_operators,data,comparisons,aliases)
                        if(len(join)>0):
                            new_data = joining(join_keywords,join,aliases,table_attrs,data,lookup_attrs)
                        
                        if(stack_attr):
                            new_data = grouping(stack_attr,new_data)
                        if(len(all_aggregators)>0):
                            # print('aggregating')
                            target,attribute = aggr_prep(all_aggregators,new_data)
                            # print('target:',target)
                            new_data = new_data[new_data[attribute]==target]
                            # print('chunk output')
                            # print(new_data)
                            
                            
                        if(len(ordered)>0):

                            new_data = aggr_prep(ordered,new_data)
                            
                        # print('chunk output:')
                        # print(new_data)
                            
                        final_data = pd.concat([final_data,new_data]).reset_index(drop=True)
                        j+=1
                    except StopIteration:
                        break
                k+=1
            except StopIteration:
                break

        
    else:
        itr_object = get_outer_loop(table_names[0],table_attrs[table_names[0]])
        iterator = iter_outer_loop(itr_object)
        
        while True:
            try:
                data[table_names[0]] = next(iterator)
                data[table_names[0]].reset_index(drop=True,inplace=True)
                # print('Sending next chunk:')
                # print(data)
                # print('chunk memory usage:',data[table_names[0]].info(memory_usage='deep'))
                
                new_data = filterout(aggr_functions,have_comparisons,filter_operators,data,comparisons,aliases)
                if(len(join)>0):
                    new_data = joining(join_keywords,join,aliases,table_attrs,data,lookup_attrs)
                    # print('chunk output:')
                    # print(new_data)
                
                if(stack_attr):
                    new_data = grouping(stack_attr,new_data)
                if(len(all_aggregators)>0):
                    target,attribute = aggr_prep(all_aggregators,new_data)
                    new_data = new_data[new_data[attribute]==target]
                    # print('target:',target)
                    # print('chunk output:')
                    # print(new_data)
                    
                if(len(ordered)>0):
                    new_data = aggr_prep(ordered,new_data)
                    
                # print('chunk output:')
                # print(new_data)
                    
                final_data = pd.concat([final_data,new_data])
                
            except StopIteration:
                break

    
    if(len(all_aggregators)>0):
        # target,attribute = aggr_prep(all_aggregators,final_data)
        # final_data = final_data[final_data[attribute]==target]
        final_data,attribute = aggr_prep(all_aggregators,final_data)
        
    if(len(ordered)>0):
        # print('final ordering')
        final_data = aggr_prep(ordered,final_data)
        
    attrs_to_show = []
    for i in lookup_attrs:
        for j in lookup_attrs[i]:
            attrs_to_show.append(j)
            
    if(attrs_to_show[0]!='all' and type(final_data)!=np.int64):
        # print('attrs to show:',attrs_to_show)
        # print('final data type:',type(final_data))
        # print('final data:',final_data)
        print(final_data[attrs_to_show])
        # print(final_data.info(memory_usage='deep'))
        
    else:
        print(final_data)
    # print('final data columns:',final_data.columns)
        # print('final data memory usage:',final_data.info(memory_usage='deep'))
            
    return final_data

def grouping(stack_attr,new_data):
    
    attr = list(stack_attr.values())[0]
    
    attr_values = list(set(new_data[attr]))
    
    new_data2 = pd.DataFrame(columns=new_data.columns)
    
    for i in attr_values:
        subset = new_data[new_data[attr]==i]
        new_data2 = pd.concat([new_data2,subset])
        
    return new_data2
    

def overlap(left_table,left_column,right_table,right_column,data,lookup_attrs,aliases,join):
    
    left_alias = join[0]['l_operand'].split('.')[0]
    left_table_columns = list(lookup_attrs[left_alias])
    
    right_alias = join[0]['r_operand'].split('.')[0]
    right_table_columns = list(lookup_attrs[right_alias])
    
    all_columns = left_table_columns[:]
    all_columns.extend(right_table_columns)
    
    org_left_table = data[left_table]
    org_right_table = data[right_table]
    len_left_table = org_left_table.shape[0]
    len_right_table = org_right_table.shape[0]
    
    new_data = pd.DataFrame([],columns=all_columns)
    rows = []
    
    right_values = list(set(org_right_table[right_column]))
    
    for i in org_left_table[left_column]:
        for k in right_values:
            if(i==k):
                subset_left = org_left_table[org_left_table[left_column]==i]
                subset_left = subset_left[left_table_columns].iloc[0,:].values
                row = list(subset_left)
                subset_right = org_right_table[org_right_table[right_column]==i]
                subset_right = subset_right[right_table_columns].values
                for i in subset_right:
                    for j in i:
                        row.append(j)
                rows.append(row)
                break
            
    new_data = pd.DataFrame(rows,columns=all_columns)
    return new_data





def joining(join_keywords,join,aliases,table_attrs,data,lookup_attrs):
    

    left_table = aliases[join[0]['l_operand'].split('.')[0]]
    left_column = join[0]['l_operand'].split('.')[1]
    right_table = aliases[join[0]['r_operand'].split('.')[0]]
    right_column = join[0]['r_operand'].split('.')[1]
    type_of_join = join[0]['keyword']
    
    if type_of_join=='OVERLAP':
        new_data = overlap(left_table,left_column,right_table,right_column,data,lookup_attrs,aliases,join)
        return new_data    
    
def sort_df(df,attr,operation):
    
    # print('df at start:',df)
    if(operation=='LARGEST' or operation=='DESC'):
        for i in range(1, len(df)):
            key_row = df.iloc[i].copy()
            key_value = key_row[attr]
            j = i - 1
    
            while j >= 0 and key_value > df.iloc[j][attr]:
                df.iloc[j + 1] = df.iloc[j].copy()
                j -= 1
    
            df.iloc[j + 1] = key_row            
        if(operation=='LARGEST'):
            largest_value = df[attr].iloc[0]
            return largest_value
            
            
    elif(operation=='LEAST' or operation=='ASC'):
        # print('operation is:',operation)
        for i in range(1, len(df)):
            # print('in loop')
            key_row = df.iloc[i].copy()
            key_value = key_row[attr]
            j=i-1
            
            while j >= 0 and key_value < df.iloc[j][attr]:
                df.iloc[j + 1] = df.iloc[j].copy()
                j -= 1
    
            df.iloc[j + 1] = key_row
            
        if(operation=='LEAST'):
            smallest_value = df[attr].iloc[0]
            # print('smallest value:',smallest_value)
            return smallest_value
        
    # print('df in sort_df:',df)
                
    return df

    
def aggr_prep(all_aggregators,new_data):
    
    # print(all_aggregators)
    # for aggr in all_aggregators:
    operation = all_aggregators[0]['operation']
    attr = all_aggregators[0]['attr']

    if(operation=='LARGEST' or operation=='LEAST'):
        target = sort_df(new_data,attr,operation)
        return target,attr
    
    if(operation=='ASC' or operation=='DESC'):
        # print('operation in prep:',operation)
        new_data = sort_df(new_data,attr,operation)
        # print('new data in aggr_prep',new_data)
        return new_data            
            
        
    
    

def filterout(aggr_functions,have_comparisons,filter_operators,data,comparisons=False,aliases=False):
    
    if not have_comparisons:
        return list(data.values())[0]
    
    fns=0
    i=0
    ops_to_check = []
    while i < len(comparisons):
        # print('operation:',comparisons[i])
        # print('i:',i)
        if (i+1<len(comparisons)): 
            if(comparisons[i+1]=='AND' or comparisons[i+1]=='OR'):
                concat = comparisons[i+1]
                fns = 1

        if(fns):
            while(fns<3):
                if(comparisons[i]=='OR' or comparisons[i]=='AND'):
                    i+=1
                    continue
                fns+=1

                l_operand = comparisons[i]['l_operand']
                operator = comparisons[i]['operator']
                r_operand = comparisons[i]['r_operand']
                keyword = comparisons[i]['keyword']
                
                if(keyword=='FILTER'):

                    if('.' in l_operand):
                        left_alias = l_operand.split('.')
                        left_column = left_alias[1]
                        left_table = data[aliases[left_alias[0]]]
    
                    else:
                        left_table = list(data.values())[0]
                        left_column = l_operand
    
                    if(isinstance(r_operand,list)):
                        ops_to_check.append({'left_table':left_table,'left_column':left_column,'right_table':r_operand,'right_column':False,'operation':operator})
    
    
                    # Checking if r_operand is a column
                    elif(len(data)==1 and r_operand in list(list(data.values())[0].columns)):
                        right_table = list(data.values())[0]
                        right_column = r_operand
                        ops_to_check.append({'left_table':left_table,'left_column':left_column,'right_table':right_table,'right_column':right_column,'operation':operator})
    
    
    
                    elif('.' in r_operand):
                        right_alias = r_operand.split('.')
                        right_column = data[aliases[right_alias[0]]][right_alias[1]]
                        right_table = data[aliases[right_alias[0]]]
                        ops_to_check.append({'left_table':left_table,'left_column':left_column,'right_table':right_table,'right_column':right_column,'operation':operator})
    
    
                    elif(r_operand[0]=="'"):
                        # print('r_operand:',r_operand)
                        # print('left column:',left_column)
                        new_data = left_table[left_table[left_column]==r_operand[1:-1]]
                        ops_to_check.append({'left_table':left_table,'left_column':left_column,'right_table':r_operand[1:-1],'right_column':False,'operation':operator})
    
    
                    elif(r_operand.isdigit()):
                        new_data = left_table[left_table[left_column]==r_operand]
                        ops_to_check.append({'left_table':left_table,'left_column':left_column,'right_table':int(r_operand),'right_column':False,'operation':operator})
                        

                elif(keyword in aggr_functions):
                    
                    if('.' in l_operand):
                        left_alias = l_operand.split('.')
                        # print('left alias:',left_alias)
                        left_column = left_alias[1]
                        left_table = data[aliases[left_alias[0]]]
            
                    else:
                        left_table = list(data.values())[0]
                        left_column = l_operand
        
                    
                    if('.' in r_operand):
                        right_alias = r_operand.split('.')
                        right_column = data[aliases[right_alias[0]]][right_alias[1]]
                        right_table = data[aliases[right_alias[0]]]
                        aggr_to_check = [{'operation':keyword,'attr':right_column}]
                        value,attribute = aggr_prep(aggr_to_check,right_table)
                        # print('value:',value)
                        ops_to_check.append({'left_table':left_table,'left_column':left_column,'right_table':value,'right_column':False,'operation':operator})
                        
                    if(len(data)==1 and r_operand in list(list(data.values())[0].columns)):
                        right_table = list(data.values())[0]
                        right_column = r_operand
                        aggr_to_check = [{'operation':keyword,'attr':r_operand}]
                        value,attribute = aggr_prep(aggr_to_check,right_table)
                        # print('target value:',value)
                        ops_to_check.append({'left_table':left_table,'left_column':left_column,'right_table':value,'right_column':False,'operation':operator})
                    
                i+=1
                
            # print('concat:',concat)
            if(concat=='AND'):
                new_data = check_and(ops_to_check,aliases)
                return new_data
            elif(concat=='OR'):
                new_data = check_or(ops_to_check,aliases)
                # print('Returning new data')
                return new_data

            fns=0
        l_operand = comparisons[i]['l_operand']
        operator = comparisons[i]['operator']
        r_operand = comparisons[i]['r_operand']
        keyword = comparisons[i]['keyword']
        
        if(keyword=='FILTER'):


            if('.' in l_operand):
                left_alias = l_operand.split('.')
                left_column = left_alias[1]
                left_table = data[aliases[left_alias[0]]]
    
            else:
                left_table = list(data.values())[0]
                left_column = l_operand
    
    
    
            # Checking if r_operand is a column
            if(len(data)==1 and r_operand in list(list(data.values())[0].columns)):
                right_table = list(data.values())[0]
                right_column = r_operand
                ops_to_check.append({'left_table':left_table,'left_column':left_column,'right_table':right_table,'right_column':right_column,'operation':operator})
    
    
    
            elif('.' in r_operand):
                right_alias = r_operand.split('.')
                right_column = data[aliases[right_alias[0]]][right_alias[1]]
                right_table = data[aliases[right_alias[0]]]
                ops_to_check.append({'left_table':left_table,'left_column':left_column,'right_table':right_table,'right_column':right_column,'operation':operator})
    
    
            elif(r_operand[0]=="'"):
                ops_to_check.append({'left_table':left_table,'left_column':left_column,'right_table':r_operand[1:-1],'right_column':False,'operation':operator})
    
            elif(isinstance(r_operand,list)):
                ops_to_check.append({'left_table':left_table,'left_column':left_column,'right_table':r_operand,'right_column':False,'operation':operator})
    

            new_data = ops_prep(ops_to_check[0],aliases)
            i+=1
            
        elif(keyword in aggr_functions):
            
            if('.' in l_operand):
                left_alias = l_operand.split('.')
                left_column = left_alias[1]
                left_table = data[aliases[left_alias[0]]]
    
            else:
                left_table = list(data.values())[0]
                left_column = l_operand

            
            if('.' in r_operand):
                right_alias = r_operand.split('.')
                right_column = data[aliases[right_alias[0]]][right_alias[1]]
                right_table = data[aliases[right_alias[0]]]
                aggr_to_check = [{'operation':keyword,'attr':right_column}]
                value = aggr_prep(aggr_to_check,right_table)
                # print('value:',value)
                new_data = check_operation(left_table,left_column,value,operator)
                return new_data
                
            if(len(data)==1 and r_operand in list(list(data.values())[0].columns)):
                right_table = list(data.values())[0]
                right_column = r_operand
                aggr_to_check = [{'operation':keyword,'attr':r_operand}]
                value,attribute = aggr_prep(aggr_to_check,right_table)
                new_data = check_operation(left_table,left_column,value,operator)
                
        i+=1
            
        
    return new_data

        

def parse(tokens,keywords,functions,filter_operators,join_keywords,aggr_functions,ordering_functions):
    #Creating a dictionary where table names will be keys and the attrs to be called will be values
    table_attrs = dict()
    # Searching for the keywprd INSIDE
    inside = functions['INSIDE']
    aliases = dict()
    comparisons = []
    join = []
    stackby = functions['STACKBY']
    stack_attr = dict()
    lookup_attrs = dict()
    all_aggrs = []
    ordering = []

    # Looping through words between INSIDE and end of query to get all the table names
    for i in tokens[inside+1:]:
        if(i in keywords or i in filter_operators or i in aggr_functions or i in ordering_functions or ord(i[0])<65 or ord(i[0])<97):
            continue

        table = i.split('.')
        if(len(table)>1):
            if(table[0] not in aliases):

                aliases[table[0]] = table[1]
                table_attrs[table[1]] = set()
        else:
            table_name = i
            table_attrs[i] = set()
            break

    # Looping through words after LOOKUP to get all the attributes
    
    # Getting only those attributes that will be displayed
    for i in range(1,inside):
        # print('token:',tokens[i])
        if(tokens[i] in keywords or tokens[i] in filter_operators or (ord(tokens[i][0])<65 and ord(tokens[i][0])!=40)   or ord(tokens[i][0])<97):
            
            if(ord(tokens[i][0])==40):
                
                table = tokens[i].split('.')
                if(len(table)>1):
                    if(table[0] not in lookup_attrs):
        
                        lookup_attrs[table[0]] = set()
                        lookup_attrs[table[0][1:]].add(table[1][:-1])
                        continue
                        
                    else:
                        lookup_attrs[table[0][1:]].add(table[1][:-1])
                        continue
                else:
                    lookup_attrs[table_name] = set()
                    lookup_attrs[table_name].add(table[0][1:-1])
                    continue
                    
            else:
                continue

        table = tokens[i].split('.')
        if(len(table)>1):
            if(table[0] not in lookup_attrs):

                lookup_attrs[table[0]] = set()
                lookup_attrs[table[0]].add(table[1])
                
            else:
                lookup_attrs[table[0]].add(table[1])
        else:
            if(table_name not in lookup_attrs):
                lookup_attrs[table_name] = set()
                lookup_attrs[table_name].add(table[0])
            # break
            
            else:
                lookup_attrs[table_name].add(table[0])
        
    # print('lookup attrs:',lookup_attrs)
    
    # Stackby
    if (stackby):
        attr = tokens[stackby+1]
        attr = attr.split('.')
        if(len(attr)>1):
            stack_attr[attr[0]] = attr[1]
            
        else:
            stack_attr[table_name] = attr[0]
            
        
    j=0
    while(j<len(tokens)):
        if((tokens[j] not in keywords and tokens[j] not in filter_operators and tokens[j] not in join_keywords and tokens[j] not in aggr_functions and tokens[j] not in ordering_functions and (ord(tokens[j][0])>=65 and ord(tokens[j][0])<=90) or (ord(tokens[j][0])>=97 and ord(tokens[j][0])<=122)) or tokens[j][0]=='('):
            attr = tokens[j][:]
            if(tokens[j][0]=='('):
                attr = tokens[j][1:-1]
                aggr = dict()
                aggr['operation'] = tokens[j-1]
                aggr['attr'] = tokens[j][1:-1]
                aggr['new_name'] = aggr['operation']+'_'+aggr['attr']
                all_aggrs.append(aggr)
                
            if(tokens[j-1] in ordering_functions):
                ordered = dict()
                ordered['attr'] = attr
                ordered['operation'] = tokens[j+1]
                ordering.append(ordered)
                
            attr = attr.split('.')
            

            
            if(len(attr)>1):
                if(aliases[attr[0]] in table_attrs and aliases[attr[0]]!=attr[1]):
                    table_attrs[aliases[attr[0]]].add(attr[1])
                    j+=1
                    continue
                else:
                    if(attr[0] not in aliases):
                        aliases[attr[0]] = attr[1]
                        table_attrs[aliases[attr[0]]] = set()
                        table_attrs[aliases[attr[0]]].add(attr[1])
                        j+=1
                        continue
                    
                    else:
                        j+=1
                        continue
                        
            else:
                if(attr[0] not in table_attrs.keys()):
                    if(len(attr[0])>1):
                        if((ord(attr[0][0])>=65 and ord(attr[0][0])<=90) or (ord(attr[0][0])>=97 and ord(attr[0][0])<=122)):
                           table_attrs[table_name].add(attr[0])
                           j+=1
                           continue
                       
                        else:
                            j+=1
                            continue
                    else:
                        if((ord(attr[0])>=65 and ord(attr[0])<=90) or (ord(attr[0])>=97 and ord(attr[0])<=122)):
                           table_attrs[table_name].add(attr[0])
                           j+=1
                           continue
                       
                        else:
                            j+=1
                            continue
                        
                else:
                    j+=1
                    continue
                

        if(tokens[j]=='AND' or tokens[j]=='OR'):
            comparisons.append(tokens[j])
            j+=1
            continue
        if(tokens[j] in keywords or tokens[j] in filter_operators or tokens[j] in join_keywords or tokens[j] in aggr_functions or tokens[j] in ordering_functions):
            j+=1
            continue

        if(tokens[j][0]=='['):
            operation = {'l_operand':tokens[j-2],'operator':tokens[j-1],'keyword':'FILTER'}
            j+=1
            values = []
            while(tokens[j][-1]!=']' and tokens[j]!=';'):
                values.append(tokens[j])
                j+=1
                if(tokens[j][-1]==']'):
                    break
            operation['r_operand'] = values
            comparisons.append(operation)
            j+=1
            continue

        elif(ord(tokens[j][0])<65 and ord(tokens[j][0])!=34 and ord(tokens[j][0])!=39):
            operation = {'l_operand':tokens[j-1],'r_operand':tokens[j+1],'operator':tokens[j],'keyword':tokens[j-2]}
            if(tokens[j-4] in join_keywords):
                operation['keyword'] = tokens[j-4]
                join.append(operation)
                j+=1
                continue
            
            elif(tokens[j+1] in aggr_functions):
                
                operation['keyword'] = tokens[j+1]
                operation['r_operand'] = tokens[j+2][1:-1]
                comparisons.append(operation)
                j+=3
            
                
            else:
                # print('filtering operation found')
                operation['keyword']='FILTER'
                comparisons.append(operation)
            j+=1
            continue

        attr = tokens[j].split('.')
        # print('attr:',attr)

        if(ord(attr[0][0])==34 or ord(attr[0][0])==39):
            j+=1
            continue


    return lookup_attrs,table_attrs,comparisons,join,stack_attr,aliases,all_aggrs,ordering

# if __name__=='__main__':
    
def driver(statement):
    
        # query = sys.argv[1]
        
    
    query1 = "LOOKUP name state INSIDE companies"
    query2 = "LOOKUP name state INSIDE companies FILTER city = 'Chicago'"
    query3 = "LOOKUP name city INSIDE companies FILTER state WITHIN ['djcnsdf' 'hdhsbdmcbd'] OR city = 'Chicago'"
    query4 = "LOOKUP c.name c.city INSIDE c.companies FILTER c.state WITHIN ['NY' 'CA' 'NJ'] AND c.city = 'New York'"
    query5 = "LOOKUP name city company_size INSIDE companies FILTER state WITHIN ['djcnsdf' 'hdhsbdmcbd'] OR company_size AMIDST ['5' '8']"
    query6 = "LOOKUP name city INSIDE companies FILTER state WITHIN ['djcnsdf' 'hdhsbdmcbd'] OR company_size > '4'"
    query7 = "LOOKUP c.company_id c.name ci.industry INSIDE c.companies ci.company_industries OVERLAP ci.company_industries UPON ci.company_id = c.company_id"
    # query8 = "LOOKUP c.name c.city INSIDE c.companies FILTER c.state WITHIN ['NY' 'CA' 'NJ']"
    query9 = "LOOKUP c.name ci.industry INSIDE c.companies ci.company_industries RJOIN ci.company_industries UPON ci.company_id = c.company_id"
    # query10 = "LOOKUP c.name ci.industry INSIDE c.companies ci.company_industries LJOIN ci.company_industries UPON ci.company_id = c.company_id"
    query11 = "LOOKUP industry company_id INSIDE company_industries STACKBY industry"
    # query12  = "LOOKUP c.company_id c.name ci.industry INSIDE c.companies ci.company_industries OVERLAP ci.company_industries UPON ci.company_id = c.company_id STACKBY ci.industry"
    query13 = "LOOKUP LEAST (company_size) INSIDE companies"
    query14 = "LOOKUP LARGEST (company_size) INSIDE companies"
    # query15 = "LOOKUP company_size INSIDE companies FILTER company_size > LEAST (company_size)"
    # query16 = "LOOKUP name company_size INSIDE companies FILTER company_size > LEAST (company_size)"
    query17 = "LOOKUP name company_size INSIDE companies ORDBY company_size ASC"
    # query18 = "LOOKUP name company_size INSIDE companies FILTER state WITHIN ['NY' 'CA' 'NJ'] AND company_size > LEAST (company_size)"
    
    query19 = "INSERT INTO companies VALUES ['3' 'comp1' 'desc1' '10' 'CA' 'US' 'LA' '10005' 'addr1' 'url1'] ['4' 'comp2' 'desc2' '11' 'TN' 'US' 'TN' '30005' 'addr2' 'url2']"
    # query20 = "ADD TABLE table1 [attr1 INT PRIMARY] [attr2 INT] [attr3 VARCHAR]"
    # query20_1 = "ADD TABLE table1 attr1 attr2 attr3"
    # query21 = "REMOVE table table1"
    # query22 = "UPDATE companies SET company_id = 'aaaa' FILTER city = 'New York'"
    query23 = "UPDATE companies DELETE FILTER city = 'New York'"
    # query24 = "UPDATE companies SET company_id = 'aaaa'"
    query25 = "LOOKUP all INSIDE companies"
    query26 = "LOOKUP name company_size INSIDE companies STACKBY company_size HAVING company_size > '5'"
    
    # query = query26.strip()
    query = statement.strip()
    # print('query:',query)
    tokens = [p for p in re.split("( |\\\".*?\\\"|'.*?')", query) if p.strip()]
    # print('tokens:',tokens)
    # breakpoint()
    keywords = ['LOOKUP','INSIDE','STACKBY','FILTER','WITHIN','AMIDST','AND',
                'OR','UPON','HAVING']
    
    functions = {'FILTER':0,'LEAST':0,'LARGEST':0,'STACKBY':0,'OVERLAP':0,
                 'INSIDE':0,'AND':0,'OR':0,'STACKBY':0}
    
    db_update_keywords = ['INSERT','REMOVE','UPDATE','ADD']
    
    
    aggr_functions = {'LARGEST':0,'LEAST':0,'ASC':0,'DESC':0}
    
    ordering_functions = {'ORDBY':0}
    
    
    join_keywords = {'OVERLAP':0}
    
    filter_operators = {'WITHIN':0,'AMIDST':0}
    
    update_keywords = {'FILTER':0}
    
    functions,filter_operators = check_ops(tokens,functions,filter_operators)
    # print('functions:',functions)
    
    have_comparisons = 0
    
    if(tokens[0]=='LOOKUP'):
        lookup_attrs,table_attrs,comparisons,join,stack_attr,aliases,all_aggregators,ordered = parse(tokens,keywords,functions,filter_operators,join_keywords,aggr_functions,ordering_functions)
        
        if comparisons:
            have_comparisons=True
    
        # print('table attrs:',table_attrs)
        # print('comparisons:',comparisons)
        # print('aggregations:',all_aggregators)
        df = get_table_attrs(table_attrs,lookup_attrs,comparisons,join,stack_attr,aliases,all_aggregators,ordered,tokens,keywords,functions,filter_operators,join_keywords,aggr_functions,ordering_functions,have_comparisons)
        
    else:
        if(tokens[0]=='INSERT'):
            check_rows(tokens)
            
        if(tokens[0]=='ADD'):
            # add_table(tokens)
            # print('add tokens:',tokens)
            add_table(tokens)
            
        if(tokens[0]=='REMOVE'):
            delete_table(tokens)
            
        if(tokens[0]=='UPDATE'):
            update_table(tokens)
            
    # return df
        

if __name__=='__main__':
    while True:
        statement = input("MyDB>")
        if(statement=='exit'):
            print('bye')
            break
        driver(statement)