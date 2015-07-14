# -*- coding: utf-8 -*-
# pansharp using Gram-schmidt algorithm
# Yichuan Shi 
# 14 July 2015
# ---------------------------------------------------------------------------

import os

# Import arcpy module
import arcpy
arcpy.env.overwriteOutput = True

# Script arguments

def pansharpen(folder):
	# init
	arcpy.env.workspace = folder

	nir_band, r_band, g_band, b_band, p_band = None, None, None, None, None

	# find relevant file
	for eachfile in os.listdir(folder):
		if eachfile.upper().endswith('B5.TIF'):
			nir_band = eachfile.upper()
		elif eachfile.upper().endswith('B4.TIF'):
			r_band = eachfile.upper()
		elif eachfile.upper().endswith('B3.TIF'):
			g_band = eachfile.upper()
		elif eachfile.upper().endswith('B2.TIF'):
			b_band = eachfile.upper()
		elif eachfile.upper().endswith('B8.TIF'):
			p_band = eachfile.upper()
		else:
			pass

	# make sure all bands are available
	if nir_band and r_band and g_band and b_band and p_band:
		temp_composite = r'in_memory\temp_composite'	# in_memory space 

		# composite bands first
		try:
			arcpy.CompositeBands_management(';'.join([r_band, g_band, b_band, nir_band]), temp_composite)
		except Exception as e:
			print(e)
			return None

		# pansharpen
		try:
			# construct new text
			out_tif = os.path.splitext(nir_band)[0].split('_')[0] + '_' + 'PAN432' + '.tif'

			# create output
			arcpy.CreatePansharpenedRasterDataset_management(temp_composite, "1", "2", "3", "4", out_tif, p_band, "Gram-Schmidt", "0.42", "0.51", "0.07", "0", "Landsat 8")

		except Exception as e:
			print(e)
			return None

		# clean up
		arcpy.Delete_management(temp_composite)

		return out_tif

	else:
		print('Error: some bands are missing, exit')
		return None

def folder_has_pansharpen(folder):
	for eachfile in os.listdir(folder):
		if eachfile.endswith('PAN432.tif'):
			return True
		else:
			pass

	return False

	
