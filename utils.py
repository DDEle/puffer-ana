import json


def get_expt_settings(f_name):
    expt_settings = {}
    with open(f_name) as f:
        ls = f.readlines()

    for line in ls:
        idx, s = line.split(" ", 1)
        doc = json.loads(s)

        name = doc.get("abr_name")
        if not name:
            name = doc.get("abr")
        group = f"{name}/{doc.get('cc')}"
        doc["group"] = group
        expt_settings[int(idx)] = doc
    return expt_settings
