


import os
os.environ['BRT_ENV'] = 'dev'

import brtdevkit
from brtdevkit.data import LabelMap


# create new Label Map
new_label_map = LabelMap.create(**{
    "name": "patrik_labelmap3",
    "class_map": { '1': 'Person','2': 'Teen','3': 'Boy','4': 'Male'}
})
new_label_map