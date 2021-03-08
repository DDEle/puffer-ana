
expt_ssim_sum = {}
expt_count = {}
for df in pd.read_csv(VIDEO_SENT, sep=',', chunksize=1000000):

    # Raw ssim to db: -10.0 * log10( 1 - raw_ssim );
    ssim_sum = df.loc[:, ("expt_id", "ssim_index")].groupby(
        "expt_id").agg(['sum', 'count'])
    for expt_id, r in ssim_sum.iterrows():
        if expt_id in expt_ssim_sum:
            expt_ssim_sum[expt_id] += r.loc["ssim_index"].loc["sum"]
            expt_count[expt_id] += r.loc["ssim_index"].loc["count"]
        else:
            expt_ssim_sum[expt_id] = r.loc["ssim_index"].loc["sum"]
            expt_count[expt_id] = r.loc["ssim_index"].loc["count"]
        print(expt_ssim_sum.keys())

# not group abr
# expt_ssim_mean = {k: expt_ssim_sum[k] / expt_count[k]
#                   for k in expt_ssim_sum.keys()}

# group abr
groups = {}
for expt_id in expt_count.keys():
    g = expt_set[expt_id]["group"]
    if g not in groups:
        groups[g] = set()
    groups[g].add(expt_id)

expt_group_count = {g: sum(expt_count[e]
                           for e in groups[g]) for g in groups.keys()}
expt_group_ssim_mean = {g: sum(expt_ssim_sum[e] for e in groups[g]) / expt_group_count[g]
                        for g in groups.keys()}
expt_ssim_mean = {
    k: expt_group_ssim_mean[expt_set[k]["group"]] for k in expt_ssim_sum.keys()}

# second pass to calculate std
expt_ssim_var_sum = {}
for df in pd.read_csv(VIDEO_SENT, sep=',', chunksize=1000000):
    df["ssim_var"] = (df["expt_id"].map(
        expt_ssim_mean) - df["ssim_index"]) ** 2

    ssim_sum = df.loc[:, ("expt_id", "ssim_var")].groupby(
        "expt_id").agg(['sum'])
    for expt_id, r in ssim_sum.iterrows():
        if expt_id in expt_ssim_var_sum:
            expt_ssim_var_sum[expt_id] += r.loc["ssim_var"].loc["sum"]
        else:
            expt_ssim_var_sum[expt_id] = r.loc["ssim_var"].loc["sum"]
        print(expt_ssim_var_sum.keys())
# not group abr
expt_ssim_std = {k: (expt_ssim_var_sum[k] / expt_count[k]) ** .5
                 for k in expt_ssim_var_sum.keys()}

# group abr
expt_group_ssim_std = {g: (sum(expt_ssim_var_sum[e] for e in groups[g]) / expt_group_count[g]) ** .5
                       for g in groups.keys()}

sorted_groups = sorted(groups.keys())
num_groups = len(sorted_groups)

data_dict = {}
for y in range(num_groups):
    g = sorted_groups[y]
    z95 = st.norm.ppf(.975)

    lower = ssim2db(
        expt_group_ssim_mean[g] - z95 * expt_group_ssim_std[g] / (expt_group_count[g] ** .5))
    upper = ssim2db(
        expt_group_ssim_mean[g] + z95 * expt_group_ssim_std[g] / (expt_group_count[g] ** .5))
    plt.plot((lower, upper), (y, y), 'ro-', color='orange')
plt.yticks(range(num_groups), sorted_groups)
plt.show()

print("time used: ", time.time() - t_start)


sum = 0
for df in pd.read_csv(VIDEO_SENT, sep=',', chunksize=1000000):

    # Raw ssim to db: -10.0 * log10( 1 - raw_ssim );
    ssim_sum = df.loc[:, ("expt_id", "ssim_index")].groupby(
        "expt_id").agg(['sum', 'count'])
    for expt_id, r in ssim_sum.iterrows():
        sum += r.loc["ssim_index"].loc["count"]


sum_ack = 0
for df in pd.read_csv(VIDEO_ACKED, sep=',', chunksize=1000000):
    sum_ack += len(df)
