import os
import arcpy
import YichuanM as ym
from download import landsat_scene

# pre-authored mxd file
WORKSPACE = r'E:\Yichuan\Landsat_archiving'
WDPAID = 555577555
os.chdir(WORKSPACE)

map_template = 'map_template.mxd'
data_depo = str(WDPAID)
target_folder = str(WDPAID) + '_' +  os.path.splitext(map_template)[0]

if not os.path.exists(target_folder):
	os.mkdir(target_folder)

# CONSTANT
RASTER_LAYER = 'raster_layer'
DATE_ELEMENT = 'l_date'
RESO = 200	# dpi


# init get df extent
mxd = arcpy.mapping.MapDocument(map_template)
df = arcpy.mapping.ListDataFrames(mxd)[0]
df_extent = df.extent

del mxd, df

def process_a_folder(folder):
	for eachfile in os.listdir(folder):
		# print(eachfile)
		# must have pansharpened image
		if eachfile.upper().endswith('PAN432.TIF'):
			rasterpath = folder + os.sep + eachfile
			r = arcpy.Raster(rasterpath)
			# extent of the current viewport must be entirely within the raster extent
			if r.extent.contains(df_extent):
				print('Processing: '+ eachfile)
				export_map(rasterpath)
				


def export_map(rasterpath):
	mxd = arcpy.mapping.MapDocument(map_template)
	
	# raster layer replace source
	r_layer = ym.find_layer_by_name(RASTER_LAYER, mxd)

		# replace source
	r_layer.replaceDataSource(os.path.dirname(rasterpath), 'RASTER_WORKSPACE', os.path.basename(rasterpath))

	# update text element
	date_element = ym.find_element_by_name(DATE_ELEMENT, mxd)

	# get new date
	raster_name = os.path.splitext(os.path.basename(rasterpath))[0]
	l8 = landsat_scene(raster_name)

	ym.set_txt_element_value(date_element, l8.date_text_readable())

	# output
	outpath = target_folder + os.sep + l8.date_text() + '.jpg'

	ym.export_map(outpath, 'jpg', mxd, reso = RESO)



#=======TEST========
def _test():
	for eachdir in os.listdir(data_depo):
		folder = data_depo + os.sep + eachdir
		process_a_folder(folder)



#=============
def main():

	for eachdir in os.listdir(data_depo):
		folder = data_depo + os.sep + eachdir
		process_a_folder(folder)

main()