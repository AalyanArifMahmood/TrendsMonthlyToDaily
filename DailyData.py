from datetime import datetime, timedelta, date, time
import pandas as pd
import time
from pytrends.exceptions import ResponseError
from pytrends.request import TrendReq as UTrendReq
GET_METHOD='get'
import requests


class TrendReq(UTrendReq):
    def _get_data(self, url, method=GET_METHOD, trim_chars=0, **kwargs):
        return super()._get_data(url, method=GET_METHOD, trim_chars=trim_chars, headers=headers, **kwargs)


headers = {
    'authority': 'trends.google.com',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-language': 'en-US,en;q=0.9',
    # Requests sorts cookies= alphabetically
    # 'cookie': '__utmc=10102256; __utmz=10102256.1666642961.1.1.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided); __utma=10102256.1803865839.1666642961.1666642961.1666793470.4; __utmt=1; __utmb=10102256.26.9.1666795523478; SEARCH_SAMESITE=CgQIxpUB; SID=PwhURfo0oeigce-qW1aeLaiROw61E2zsIZkJ3Bg12BNma0L7d5I-TPPYN1dJTYi2TKihWQ.; __Secure-1PSID=PwhURfo0oeigce-qW1aeLaiROw61E2zsIZkJ3Bg12BNma0L7ID_x1ilXe6TW2YBoffbS4w.; __Secure-3PSID=PwhURfo0oeigce-qW1aeLaiROw61E2zsIZkJ3Bg12BNma0L7qF_psZ-0HBJbWuLqrkgk4w.; HSID=AzwhxePWzrby3KcBZ; SSID=AkAhw7NRcbqL3U55s; APISID=LtBcIKjRVBXNGGAd/AQMXa4wx0vy7r10R9; SAPISID=Uz5cpbaUPQWp2QJq/AtI1HZHOnd6oaCK3-; __Secure-1PAPISID=Uz5cpbaUPQWp2QJq/AtI1HZHOnd6oaCK3-; __Secure-3PAPISID=Uz5cpbaUPQWp2QJq/AtI1HZHOnd6oaCK3-; AEC=AakniGPCRNMOrdVdCIvsRLdWaREBRmLVaslXJ8bA388VA2SiFoXE2MGzzyI; 1P_JAR=2022-10-26-14; NID=511=rTxwID8M_bKhI-hS6abZJnwrqUIBplbINWhZelQzE5-AZYFaXXvdNGR9bmfvfNCijzOvcSGbzAQTyRyGbc7QqlzcBL01cluMF6keiujicRFIcjElWoEE2SiP4f42jJeefiGElgDzY59YCD75ZaXpl6h4-i9J6hLL_Yp4HAmlPYH1xlKoLBCPml1uwQzNEKIvhsNpzHNa0ev8o9LF69dhGt6_EJb4Qa9yVCCUVcuI0bCJfDcMF7XhGRynHGopOFRq-vBtmAA1WEcEJmvYUa3vw_taItclH4NZo1KWkOZpmgmOgCBcyZFbBhFV5mEOJ97-aishc4qt0fNCn-cAGnnXvWH_qh7sKW2xuEifbixQ3Ybv9Nio6ph6MQgf4d0oqIpOmlkGuixp3R_uA_bQ2JjsDmsnam_UoCvK0tSugUmJ5Sl4Cs3_rUfjEaXF2FjpNpFZmRL3g9R4-0sNjcFXiixTXcmaVjx8TXA1uQGQbtHBmyYCHLpf8h2uNcQ; SIDCC=AIKkIs2D2xow_4SPlQMftlRFMbC0AwFPSL9JXBch6L93YvCV65OM3MGPUzms25o5B0vPZMuCblV7; __Secure-1PSIDCC=AIKkIs3LIn8RkfUVh7s5VD0PYYPgG6v_Q3mhNHv07D6rgPWlUTULmuc8Dy2bhcHZi4trMBqSnxw; __Secure-3PSIDCC=AIKkIs2jFoytY67gU2UwKTqW3z9DmqsH_qTLHpURVdRUMoR0jBCOmaA6qCNIzv4JAEWVL4lKoxQ',
    'referer': 'https://trends.google.com/trends/?geo=US',
    'sec-ch-ua': '"Chromium";v="106", "Microsoft Edge";v="106", "Not;A=Brand";v="99"',
    'sec-ch-ua-arch': '"arm"',
    'sec-ch-ua-bitness': '"64"',
    'sec-ch-ua-full-version': '"106.0.1370.42"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-model': '""',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36 Edg/106.0.1370.42',
}



def _fetch_data(trendreq, kw_list, timeframe='today 3-m', cat=0, geo='', gprop='') -> pd.DataFrame:
    """Download google trends data using pytrends TrendReq and retries in case of a ResponseError."""
    attempts, fetched = 0, False
    while not fetched:
        try:
            trendreq.build_payload(kw_list=kw_list, timeframe=timeframe, cat=cat, geo=geo, gprop=gprop)
        except ResponseError as err:
            print(err)
            print(f'Trying again in {60 + 5 * attempts} seconds.')
            time.sleep(60 + 5 * attempts)
            attempts += 1
            if attempts > 3:
                print('Failed after 3 attemps, abort fetching.')
                break
        else:
            fetched = True
    return trendreq.interest_over_time()


def get_daily_trend(trendreq, keyword: str, start: str, end: str, cat=0,
                    geo='', gprop='', delta=269, overlap=100, sleep=0,
                    tz=0, verbose=False) -> pd.DataFrame:
    """Stich and scale consecutive daily trends data between start and end date.
    This function will first download piece-wise google trends data and then
    scale each piece using the overlapped period.
        Parameters
        ----------
        trendreq : TrendReq
            a pytrends TrendReq object
        keyword: str
            currently only support single keyword, without bracket
        start: str
            starting date in string format:YYYY-MM-DD (e.g.2017-02-19)
        end: str
            ending date in string format:YYYY-MM-DD (e.g.2017-02-19)
        cat, geo, gprop, sleep:
            same as defined in pytrends
        delta: int
            The length(days) of each timeframe fragment for fetching google trends data,
            need to be <269 in order to obtain daily data.
        overlap: int
            The length(days) of the overlap period used for scaling/normalization
        tz: int
            The timezone shift in minute relative to the UTC+0 (google trends default).
            For example, correcting for UTC+8 is 480, and UTC-6 is -360
    """

    start_d = datetime.strptime(start, '%Y-%m-%d')
    init_end_d = end_d = datetime.strptime(end, '%Y-%m-%d')
    init_end_d.replace(hour=23, minute=59, second=59)
    delta = timedelta(days=delta)
    overlap = timedelta(days=overlap)

    itr_d = end_d - delta
    overlap_start = None

    df = pd.DataFrame()
    ol = pd.DataFrame()

    while end_d > start_d:
        tf = itr_d.strftime('%Y-%m-%d') + ' ' + end_d.strftime('%Y-%m-%d')
        if verbose: print('Fetching \'' + keyword + '\' for period:' + tf)
        temp = _fetch_data(trendreq, [keyword], timeframe=tf, cat=cat, geo=geo, gprop=gprop)
        temp.drop(columns=['isPartial'], inplace=True)
        temp.columns.values[0] = tf
        ol_temp = temp.copy()
        ol_temp.iloc[:, :] = None
        if overlap_start is not None:  # not first iteration
            if verbose: print('Normalize by overlapping period:' + overlap_start.strftime('%Y-%m-%d'),
                              end_d.strftime('%Y-%m-%d'))
            # normalize using the maximum value of the overlapped period
            y1 = temp.loc[overlap_start:end_d].iloc[:, 0].values.max()
            y2 = df.loc[overlap_start:end_d].iloc[:, -1].values.max()
            coef = y2 / y1
            temp = temp * coef
            ol_temp.loc[overlap_start:end_d, :] = 1

        df = pd.concat([df, temp], axis=1)
        ol = pd.concat([ol, ol_temp], axis=1)
        # shift the timeframe for next iteration
        overlap_start = itr_d
        end_d -= (delta - overlap)
        itr_d -= (delta - overlap)
        # in case of short query interval getting banned by server
        time.sleep(sleep)

    df.sort_index(inplace=True)
    ol.sort_index(inplace=True)
    # The daily trend data is missing the most recent 3-days data, need to complete with hourly data
    if df.index.max() < init_end_d:
        tf = 'now 7-d'
        hourly = _fetch_data(trendreq, [keyword], timeframe=tf, cat=cat, geo=geo, gprop=gprop)
        hourly.drop(columns=['isPartial'], inplace=True)

        # convert hourly data to daily data
        daily = hourly.groupby(hourly.index.date).sum()

        # check whether the first day data is complete (i.e. has 24 hours)
        daily['hours'] = hourly.groupby(hourly.index.date).count()
        if daily.iloc[0].loc['hours'] != 24: daily.drop(daily.index[0], inplace=True)
        daily.drop(columns='hours', inplace=True)

        daily.set_index(pd.DatetimeIndex(daily.index), inplace=True)
        daily.columns = [tf]

        ol_temp = daily.copy()
        ol_temp.iloc[:, :] = None
        # find the overlapping date
        intersect = df.index.intersection(daily.index)
        if verbose: print('Normalize by overlapping period:' + (intersect.min().strftime('%Y-%m-%d')) + ' ' + (
            intersect.max().strftime('%Y-%m-%d')))
        # scaling use the overlapped today-4 to today-7 data
        coef = df.loc[intersect].iloc[:, 0].max() / daily.loc[intersect].iloc[:, 0].max()
        daily = (daily * coef).round(decimals=0)
        ol_temp.loc[intersect, :] = 1

        df = pd.concat([daily, df], axis=1)
        ol = pd.concat([ol_temp, ol], axis=1)

    # taking averages for overlapped period
    df = df.mean(axis=1)
    ol = ol.max(axis=1)
    # merge the two dataframe (trend data and overlap flag)
    df = pd.concat([df, ol], axis=1)
    df.columns = [keyword, 'overlap']
    # Correct the timezone difference
    df.index = df.index + timedelta(minutes=tz)
    df = df[start_d:init_end_d]
    # re-normalized to the overall maximum value to have max =100
    df[keyword] = (100 * df[keyword] / df[keyword].max()).round(decimals=0)

    df.to_csv('/Users/aalyanmahmood/Desktop/Stat-155/Practice Problems 9/dailyData.csv')
    return df


req = TrendReq()
get_daily_trend(req, "bitcoin", "2015-01-01", "2022-10-01")
