import os
import moviepy.editor as mpe

# save current 
tempdir = os.getcwd()

# workspace
WORKSPACE = r'E:\Yichuan\Landsat_archiving'
WDPAID = 555577555

map_template = 'map_template.mxd'
data_depo = str(WDPAID)
batch_folder = WORKSPACE + os.sep + str(WDPAID) + '_' + os.path.splitext(map_template)[0]

# change workspace
os.chdir(batch_folder)

clip = mpe.ImageSequenceClip(os.listdir(), fps=1)
clip.write_gif('image_sequence.gif')

os.chdir(tempdir)

