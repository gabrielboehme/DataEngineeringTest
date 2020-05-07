import pandas as pd
import datetime
import sys
import os


#ETL Functions
def load_data(deals_filepath, contacts_filepath, companies_filepath, sectors_filepath):

    '''
    Input:
        -Filepaths for the specified files
    
    Output:
        -Variables containing the csv data in pandas dataframes 
    '''

    separator = '\t'

    deals_df = pd.read_csv(deals_filepath, separator)
    contacts_df = pd.read_csv(contacts_filepath, separator)
    companies_df = pd.read_csv(companies_filepath, separator)
    sectors_df = pd.read_csv(sectors_filepath, separator)


    return deals_df, contacts_df, companies_df, sectors_df



def etl_deals(deals_df, contacts_df):

    '''
    Input:
        -Deals_df: Dataframe with deals information like deals date, price, contact and companie.
        -Contacts_df: Dataframe with contacts information like name, companie, phone, etc.

    Output:
        -CSV file with summarized information about the deals like price, contact name and year-month date.
    '''

    #debugging column name
    contacts_df.rename({contacts_df.columns[0]:"contactsId"},axis=1,inplace=True)

    #selecting contacts_df columns to merge with deals_df
    merge_contacts = contacts_df.loc[:,['contactsId','contactsName']]

    #merging merge_contacts and deals_df to bring contacts info to deals
    deals_contacts_df = deals_df.merge(merge_contacts,how='left',left_on='contactsId',right_on='contactsId')
    
    #creating year-month date column
    deals_contacts_df['dealsDateCreated'] = pd.to_datetime(deals_contacts_df['dealsDateCreated'])
    deals_contacts_df['dealsYearMonth'] = deals_contacts_df['dealsDateCreated'].dt.strftime('%Y-%m')
    
    #removing unnecessary columns
    deals_contacts_df.drop(['dealsId', 'dealsDateCreated', 'contactsId', 'companiesId'], axis=1,inplace=True)

    return deals_contacts_df



def etl_sector_percentage(deals_df, sectors_df, companies_df):

    '''
    Input:
        -Pandas dataframes with deals, sectors and companies data.

    Output:
        -CSV file with the Companie sector name and it's percentage in the revenue
    '''
    
    #mapping sector name to companies_df and removing other columns
    companies_sectors_df = companies_df.merge(sectors_df, how='left', left_on='sectorKey', right_on='sectorKey')
    companies_sectors_df = companies_sectors_df.loc[:,['companiesId','sector']]

    #mapping sector name to deals_df
    deals_sector_df =  deals_df.merge(companies_sectors_df, how='left', left_on='companiesId', right_on='companiesId')

    #removing other columns and grouping deals revenues by sector 
    deals_sector_df = deals_sector_df.loc[:, ['dealsPrice','sector']].groupby('sector').sum()
    deals_sector_df['dealsPrice'] = deals_sector_df['dealsPrice']/deals_sector_df['dealsPrice'].sum()
    deals_sector_df.reset_index(inplace=True)
    deals_sector_df.columns = ['Sector','SectorPercentage']

    return deals_sector_df



def dump_data(deals_contacts_df, deals_sector_df):

    '''
    Input:
        -Dataframes to output in csv format

    Output:
        -Saves DataFrames to CSV files (nothing is really outputed)
    '''

    #create output directory
    try:
        os.makedirs('ETL_Output')

    except OSError:
        None 

    #create csv files
    deals_contacts_df.to_csv('ETL_Output/deals_contacts.csv', header=True, index=False)
    deals_sector_df.to_csv('ETL_Output/deals_sector.csv', header=True, index=False)

    return None


#Function the code will actualy execute
def main():

    if len(sys.argv) == 5:

        #getting filepaths from system imput
        deals_filepath, contacts_filepath, companies_filepath, sectors_filepath = sys.argv[1:]

        #loading data
        print('Loading data...')
        
        deals_df, contacts_df, companies_df, sectors_df = load_data(deals_filepath, contacts_filepath, companies_filepath, sectors_filepath)
        
        
        #Transforming data
        print('Transforming data') #Print no of rows of the raw dataset 

        deals_contacts_df = etl_deals(deals_df, contacts_df)

        deals_sector_df = etl_sector_percentage(deals_df, sectors_df, companies_df)

        print(f'Deals_contacts lenght: {len(deals_contacts_df)} rows. \n Deals_sector lenght: {len(deals_sector_df)} rows.')

        #saving data
        print('Saving data...')

        dump_data(deals_contacts_df, deals_sector_df)

        #ETL completed
        print('ETL completed!')
        
        
    else:
        print('Please provide the filepaths of the deals, contacts, companies and sectors '\
              'datasets as the first, second, third and fourth argument respectively, as '\
              ' \n\nExample: python ETL.py Data/deals.tsv Data/contacts.tsv Data/companies.tsv Data/sectors.tsv')

    

if __name__ == '__main__':
    main()
