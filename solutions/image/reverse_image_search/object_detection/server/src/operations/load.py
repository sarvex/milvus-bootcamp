import os
import sys
from diskcache import Cache

sys.path.append("..")
from config import DEFAULT_TABLE, CACHE_DIR


def get_imgs_path(path):
    pics = os.listdir(path)
    pics.sort()
    return [os.path.join(path, f) for f in pics if f.endswith('.jpg')]

def get_num_vecs(cache, model, imgs_folder):
    paths = get_imgs_path(imgs_folder)
    cache['total'] = len(paths)
    vectors = []
    obj_num = []
    for current, x in enumerate(paths, start=1):
        vecs = model.execute(x)
        vectors.extend(iter(vecs))
        obj_num.append(len(vecs))
        cache['current'] = current
    return vectors, obj_num

def match_ids_and_imgs(imgs, obj_num):
    matched_imgs = []
    for i, num in enumerate(obj_num):
        matched_imgs.extend(imgs[i] for _ in range(num))
    return matched_imgs

def format_data(ids, names):
    data = []
    for i in range(len(ids)):
        value = (str(ids[i]), names[i])
        data.append(value)
    return data

def do_load(table_name, database_path, model, mil_cli, mysql_cli):
    if not table_name:
        table_name = DEFAULT_TABLE
    cache = Cache(CACHE_DIR)
    vectors, obj_num = get_num_vecs(cache, model, database_path)
    ids = mil_cli.insert(table_name, vectors)
    mil_cli.create_index(table_name)
    imgs = get_imgs_path(database_path)
    matched_imgs = match_ids_and_imgs(imgs, obj_num)
    mysql_cli.create_mysql_table(table_name)
    mysql_cli.load_data_to_mysql(table_name, format_data(ids, matched_imgs))
    return len(ids)
