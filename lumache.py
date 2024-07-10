"""
Lumache - Python library for cooks and food lovers.
"""

__version__ = "0.1.0"

from tqdm import tqdm
import datetime
from loguru import logger
import pandas as pd

from sources.wd_imf.parse import imf_api_endpoint_correction,wd_imf_cpi_all_countries_parse
from sources.wd_imf.extract import (imf_api_data_endpoint,
                                    wd_imf_cpi_all_countries_yoy_indicators,
                                    wd_imf_cpi_all_countries_mom_indicators,
                                    wd_imf_cpi_all_countries_index_indicators)

def wd_imf_cpi_all_countries_yoy_data(historical:bool=False):
    """
    Extracts data from all IMF CPI YOY indicators.
    :param historical: if download data since 1950
    :type historical: bool

    :return: yoy df 
    :rtype: pandas.core.frame.DataFrame

    :see-also:
    data source: https://data.imf.org/?sk=4ffb52b2-3653-409a-b471-d47b46d904b5
    """ 
    logger.info('YoY Data')
    end_year = datetime.datetime.today().year
    start_year = end_year - 1
    if historical:
        start_year = 1950
    relevant_ind = wd_imf_cpi_all_countries_yoy_indicators()
    yoy_data = []
    for i in tqdm(relevant_ind):
        d =  imf_api_data_endpoint('CPI','M',i['code'],start_year,end_year)
        d = imf_api_endpoint_correction(d)
        yoy_data = yoy_data + d
    yoy = wd_imf_cpi_all_countries_parse(yoy_data, 'wd_imf_cpi_all_countries_cpi_change_yoy')
    return yoy


def wd_imf_cpi_all_countries_mom_data(historical:bool=False):
    """
    Extracts data from all IMF CPI MOM indicators.
    :param historical: if download data since 1950
    :type historical: bool

    :return: mom df 
    :rtype: pandas.core.frame.DataFrame
    
    :see-also:
    data source: https://data.imf.org/?sk=4ffb52b2-3653-409a-b471-d47b46d904b5
    """ 
    logger.info('MoM Data')
    end_year = datetime.datetime.today().year
    start_year = end_year - 1
    if historical:
        start_year = 1950
    relevant_ind = wd_imf_cpi_all_countries_mom_indicators()
    mom_data = []
    for i in tqdm(relevant_ind):
        d =  imf_api_data_endpoint('CPI','M',i['code'],start_year,end_year)
        d = imf_api_endpoint_correction(d)
        mom_data = mom_data + d
    mom = wd_imf_cpi_all_countries_parse(mom_data,'wd_imf_cpi_all_countries_cpi_change_mom')
    return mom

def wd_imf_cpi_all_countries_index_data(historical:bool=False):
    """
    Extracts data from all IMF CPI index indicators.
    :param historical: if download data since 1950
    :type historical: bool

    :return: index df 
    :rtype: pandas.core.frame.DataFrame
    
    :see-also:
    data source: https://data.imf.org/?sk=4ffb52b2-3653-409a-b471-d47b46d904b5
    """ 
    logger.info('Index Data')
    end_year = datetime.datetime.today().year
    start_year = end_year - 1
    if historical:
        start_year = 1950
    relevant_ind = wd_imf_cpi_all_countries_index_indicators()
    index_data = []
    for i in tqdm(relevant_ind):
        d =  imf_api_data_endpoint('CPI','M',i['code'],start_year,end_year)
        d = imf_api_endpoint_correction(d)
        index_data = index_data + d
    index = wd_imf_cpi_all_countries_parse(mom_data,'wd_imf_cpi_all_countries_cpi_index')
    return index

def wd_imf_cpi_all_countries_data(historical:bool=False):
    """
    Extracts data from all IMF CPI YOY, MOM and index indicators.
    :param historical: if download data since 1950
    :type historical: bool

    :return: index df 
    :rtype: pandas.core.frame.DataFrame
    
    :warning:
    Could be that historic data is too much for imf api to send so we will need to run it by pieces to populate tukan tables.

    :see-also:
    data source: https://data.imf.org/?sk=4ffb52b2-3653-409a-b471-d47b46d904b5
    """ 
    yoy = wd_imf_cpi_all_countries_yoy_data(historical)
    mom = wd_imf_cpi_all_countries_mom_data(historical)
    index = wd_imf_cpi_all_countries_index_data(historical)
    df = pd.concat([yoy,mom,index])
    return df
