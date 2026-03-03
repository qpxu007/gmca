#python xds.py /mnt/beegfs/DATA/esaf278897/T4DNAL/UWCC11/G07/collect/T4DNAL-SBF1-G7_run8_master.h5 --optimization --user_resolution_cutoff=2.8
#python xds.py /mnt/beegfs/DATA/esaf278897/T4DNAL/UWCC11/G07/collect/T4DNAL-SBF1-G7_run8_master.h5 \
#--optimization --user_resolution_cutoff=2.8 \
#--user_space_group=p212121 \
#--user_end=400
python xds.py /mnt/beegfs/DATA/esaf278897/T4DNAL/UWCC11/G07/collect/T4DNAL-SBF1-G7_run8_master.h5 \
--user_resolution_cutoff=2.8 \
--user_space_group=p212121 --user_unit_cell="35.743    87.458   185.052  90.000  90.000  90.000" \
--user_end=400 --nxds

