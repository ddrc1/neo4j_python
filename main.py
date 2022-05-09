import similarities
import centralities
import time
import pandas as pd

graph = pd.read_csv("./graph.csv")
records = []
start = time.time()
for row in graph.itertuples():
    action_1 = row[1]
    prop_link1 = list(row[5:])
    for row2 in graph.iloc[action_1+1:].itertuples():
        action_2 = row2[1]
        prop_link2 = list(row2[5:])

        records.append({"action_1": action_1, "action_2": action_2, 
                        "jaccard": similarities.jaccard_similarity(prop_link1, prop_link2),
                        "overlap": similarities.overlap_similarity(prop_link1, prop_link2),
                        "cosine": similarities.cosine_similarity(prop_link1, prop_link2),
                        "pearson": similarities.pearson_similarity(prop_link1, prop_link2),
                        "euclidean": similarities.euclidean_similarity(prop_link1, prop_link2),
                        "euclideanDistance": similarities.euclideanDistance_similarity(prop_link1, prop_link2)})
        #print(action_1, action_2)
end = time.time()
print("Done in", end - time, "s")

pd.DataFrame(records).to_csv("./similarities.csv", index=False)
#raimir.holanda@gmail.com
