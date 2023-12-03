import argparse
import pandas as pd
import numpy as np
import glob
import os
import json

pd.set_option('display.max_columns', None)



def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="C:/Users/shijas/PycharmProjects/revolve/input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="C:/Users/shijas/PycharmProjects/revolve/input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="C:/Users/shijas/PycharmProjects/revolve/input_data/starter/transactions/d=2018-12-01/transactions.json")
    parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
    return vars(parser.parse_args())



def main():
    params = get_params()
    #Read the input files
    print("Read the input files")
    print(params['customers_location'])
    print(params['products_location'])
    print(params['transactions_location'])
    path_trans = params['transactions_location']
    path_cust = params['customers_location']
    path_pro = params['products_location']
    df_cust = pd.read_csv(path_cust)
    df_pro = pd.read_csv(path_pro)

    print(df_cust)

    #transaction file path
    path_to_json = 'C:/Users/shijas/PycharmProjects/revolve/input_data/starter/transactions/*'

    #output location
    output_data ="C:/Users/shijas/PycharmProjects/revolve/output_data/op.json"

    json_pattern = os.path.join(path_to_json, '*.json')
    file_list = glob.glob(json_pattern)
    print("printing files")
    print(file_list)


    dfs = []  # an empty list to store the data frames
    for file in file_list:
        data = pd.read_json(file, lines=True)  # read data frame from json file
        dfs.append(data)  # append the data frame to the list

    Transaction_DF = pd.concat(dfs, ignore_index=True)  # concatenate all the data frames in the list.
    # reading transaction json files
    Transaction_DF["purchase_count"] = ""
    print(Transaction_DF)
    for index, row in Transaction_DF.iterrows():
        Transaction_DF.at[index, 'purchase_count'] = len(row["basket"])
        Transaction_DF.at[index, 'product_id'] = row["basket"][0]["product_id"]

    print(Transaction_DF)

    df_cust["c_customer_id"] = df_cust["customer_id"]
    #df_cust = df_cust.drop(df_cust["customer_id"],axis=1)

    #join transaction DF with Customer DF
    Trans_cust_df = Transaction_DF.merge(df_cust,left_on="customer_id",right_on= "c_customer_id")
    Trans_cust_df = Trans_cust_df[["c_customer_id","product_id","purchase_count","loyalty_score"]]
    print(Trans_cust_df)

    #joining the resultant DF with product DF
    finalDF = Trans_cust_df.merge(df_pro,on="product_id")
    finalDF = finalDF[["c_customer_id","product_id","purchase_count","loyalty_score","product_category"]]
    print(finalDF)

    #writing finalDF to json file
    finalDF.to_json(output_data)


if __name__ == "__main__":
    main()
