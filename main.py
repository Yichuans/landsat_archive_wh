import os
import datetime as dt
import wget
import pandas as pd
import gzip

# logging
import logging
logging.basicConfig(filename='log.txt', level=logging.DEBUG, format='%(asctime)s %(message)s')

def log_print(txt):
    print(txt)
    logging.info(txt)


# constant
LOCAL_INDEX_FILE = "scene_list.gz"
LOCAL_INDEX_FILE = 'local_index.gz'

WH_WRS = 'wh_wrs.csv'
GZ_INDEX = "http://landsat-pds.s3.amazonaws.com/scene_list.gz"
CLOUD_COVER_THRES = 0.3

def get_wh_wrs_mk2():
    def add_zeros_wrs(input_string):
        if len(input_string) == 1:
            return '00'+input_string
        elif len(input_string) == 2:
            return '0' + input_string
        else:
            return input_string
    # use csv model to avoid comma-within-quotes issue
    import csv
    wrs_dict = dict()
    with open(WH_WRS, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            if row[1] not in wrs_dict.keys():
                wrs_dict[row[1]] = [row[2], []]

            wrs_dict[row[1]][1].append((add_zeros_wrs(row[3]), add_zeros_wrs(row[4])))

    return wrs_dict


def julian_day_to_date(jday, year):
    new_date = dt.datetime(year, 1, 1) + dt.timedelta(jday - 1) # starting point 0
    return new_date

class landsat_scene:
    s3 = r"http://landsat-pds.s3.amazonaws.com/L8"

    """Landsat scene - to be instantiated with the unique Landsat ID"""
    def __init__(self, id_str):
        self.lid = id_str           # landsat id
        self.sensor = id_str[0:3]
        self.wrspath = id_str[3:6]
        self.wrsrow = id_str[6:9]
        self.year = id_str[9:13]
        self.jday = id_str[13:16]   # julian day
        self.gsi = id_str[16:19]    # ground
        self.av = id_str[-2:]       # archive version

    def about(self):
        string_txt = 'Sensor: '+ self.sensor + '\n' + \
        'WRS path: '+ self.wrspath + '\n' + \
        'WRS row: '+ self.wrsrow + '\n' + \
        'Year: '+ self.year + '\n' + \
        'Julian day: '+ self.jday + '\n' + \
        'Gound station: '+ self.gsi + '\n' + \
        'Archive version: '+ self.av + '\n'

        print(string_txt)

    def date(self):
        return julian_day_to_date(int(self.jday), int(self.year))

    def date_text(self):
        return self.date().strftime('%Y%m%d')

    def save(self, archive_directory, download_all=False):
        """Download the landsat scene to the given archive_directory"""
        import urllib
        import bs4

        # archive folder
        if not os.path.exists(archive_directory):
            os.mkdir(archive_directory)

        # date as folder 
        target_dir = archive_directory + os.sep + self.date_text() + '_' + self.wrspath + self.wrsrow

        # target_dir = archive_directory + os.sep + self.date_text() + os.sep + self.wrspath + self.wrsrow
        if not os.path.exists(target_dir):
            os.mkdir(target_dir)

        # bs4 to retrieve links
        url = landsat_scene.s3 + '/' + self.wrspath + '/' + self.wrsrow + '/' + self.lid
        url_index = url + '/' + 'index.html'

        # open url
        try:
            doc = urllib.request.urlopen(url_index)
        except:
            print('Error: cannot open url ' + url_index)
            return 0
        else:
            soup = bs4.BeautifulSoup(doc)

        # download every link
        if download_all:
            try:
                for link in soup.find_all('a'):
                    downloadlink = url + '/' + link.get('href')
                    print('Downloading: '+ downloadlink)
                    wget.download(downloadlink, out=target_dir)

                print('Complete')
            except:
                return 0
            else:
                return 1

        # only thumb big
        else:
            try:
                for link in soup.find_all('a'):
                    if 'jpg' in link.get('href'):
                        downloadlink = url + '/' + link.get('href')
                        print('Downloading: '+ downloadlink)
                        wget.download(downloadlink, out=target_dir)

                print('Complete')
            except:
                return 0
            else:
                return 1


def find_record_local_index(local_index_file=LOCAL_INDEX_FILE):
    """check local index, in order to compare, return a list of LID"""
    gz_file = local_index_file

    return fetch_all_records(gz_file)


def find_record_s3landsat_index(remote_index=GZ_INDEX):
    """Unzip gz index file and return IDs given wrspath and wrsrow,
    return a list of LID"""
    # download gz file
    print('Retrieving Landsat8 index on Amazon s3...')
    gz_file = wget.download(remote_index)
    result = fetch_all_records(gz_file)
    # clean up
    os.remove(gz_file)

    return result


def fetch_record_by_wrs(wrspath, wrsrow, df):
    # find relevant rows
    val = (df['path']==wrspath)&(df['row']==wrsrow)

    # return dataframe of the found record
    return df.ix[val]

def fetch_record_by_lid(lid, df):
    val = df['entityId'] == lid
    return df.ix[val]


def fetch_all_records(gz_file):
    import gzip
    gz = gzip.open(gz_file)
    df = pd.read_csv(gz)

    # make sure no duplicates are present
    df = df.drop_duplicates()

    # set index for ease of filtering
    df = df.set_index(df['entityId'])

    return df

def fetch_disjoint(df_remote, df_local):
    # find lid that are in the remote but not
    filter_index = set(df_remote.entityId).symmetric_difference(df_local.entityId)

    # use lid to filter df, making use of reindex
    return df_remote.reindex(filter_index)

def filter_cloud(df):
    # must be lower than cloud threshold

    val = df['cloudCover'] <= CLOUD_COVER_THRES
    return df.ix[val]


#### ==========TEST===================
def _start():
    # local
    local_records = fetch_all_records(LOCAL_INDEX_FILE)

    # remote
    a = wget.download(GZ_INDEX)
    remote_records = fetch_all_records(a)
    os.remove(a)

    return local_records, remote_records

def _test_sym_dif():
    df_local = find_record_local_index(227, 61)
    df_remote = find_record_s3landsat_index(227, 61)
    return fetch_disjoint(df_remote, df_local)

def _download():
    lid = 'LC81390452014295LGN00'
    l8 = landsat_scene(lid)
    l8.save('a_test_wh_site')


### ================MAIN===============
def run_a_wh_site(wdpaid = 555577555, workspace = r'E:\Yichuan\Landsat_archiving'):
    # workspace
    os.chdir(workspace)

    # remote index and save as pandas dataframe
    df_remote = find_record_s3landsat_index(GZ_INDEX)

    # local index df
    if not os.path.exists(LOCAL_INDEX_FILE):
        print('No local index found, create an empty index')
        df_local = pd.DataFrame(columns=df_remote.columns)

    else:
        df_local = find_record_local_index(LOCAL_INDEX_FILE)

    # each WH site ===================

    # get wh wrs look-up table
    wh_wrs = get_wh_wrs_mk2()
    # okavango delta
    site_name = wh_wrs[str(wdpaid)][0]
    wrs_list = wh_wrs[str(wdpaid)][1]
    print('Processing %s'%site_name)
    print('='*(len(site_name) + 11))

    # loop each path+row combinations for the given WH site
    for wrspath, wrsrow in wrs_list:
        print('Processing path %s row %s'%(wrspath, wrsrow))

        # find corresponding records by wrs path and row. Functions take in numerics
        df_local_sub = fetch_record_by_wrs(int(wrspath), int(wrsrow), df_local)
        df_remote_sub = fetch_record_by_wrs(int(wrspath), int(wrsrow), df_remote)

        # find what's in remote but not local
        df_to_download = fetch_disjoint(df_remote_sub, df_local_sub)
        if df_to_download.size > 0:
            print('Found images available for path %s and row %s: '%(wrspath, wrsrow)\
             + str(df_to_download.size))

        else:
            print('No images found')
            continue

        # check cloud cover
        df_to_download = filter_cloud(df_to_download)
        if df_to_download.size > 0:
            print('Images meeting cloud threshold %s: '%(CLOUD_COVER_THRES,) + str(df_to_download.size))

            # download
            for lid in list(df_to_download['entityId']):
                landsat8 = landsat_scene(lid)
                landsat8.save(str(wdpaid), True)

                # make sure local index table is updated
                df_local = pd.concat([df_local, fetch_record_by_lid(lid, df_to_download)])


            # write to disk
            with gzip.open(LOCAL_INDEX_FILE, 'wt') as f:
                df_local.to_csv(f)

        else:
            print('Images do not meet cloud threshold %s'%(CLOUD_COVER_THRES,))


if __name__ == '__main__':
    run_a_wh_site()

