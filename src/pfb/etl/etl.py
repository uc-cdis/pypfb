import os
import json
from fastavro import reader


class ETL:
    def __init__(self, pfbfile, root_name):
        self.pfbfile = pfbfile
        self.root_name = root_name
        self.links = {}
        self.root_node_ids = []

    def __iter__(self):
        pass

    def transform(self):
        with open(self.pfbfile, "rb") as fo:
            avro_reader = reader(fo)
            # skip schema
            avro_reader.next()
            # iterate each record
            for record in avro_reader:
                self._process(record)

    def _process(self, record):
        data = record["object"]
        relations = record["relations"]
        submitter_id = data["submitter_id"]
        for relation in relations:
            # {u'dst_id': u'participant_metalinguistics_monofilm', u'dst_name': u'participant'}
            if relation["dst_id"] not in self.links:
                self.links[relation["dst_id"]] = []
            if (
                relation["dst_id"].split("_")[0] == self.root_name
                and relation["dst_id"] not in self.root_node_ids
            ):
                self.root_node_ids.append(relation["dst_id"])
            self.links[relation["dst_id"]].append(submitter_id)

    def get_id(self, L):
        tmp = ""
        for l in L:
            tmp = tmp + "\t" + l
        return tmp

    def check_and_add_candidate(self, sorted_list, voc_tree_result):
        for l in sorted_list:
            if l not in voc_tree_result:
                voc_tree_result[l] = {}
            voc_tree_result = voc_tree_result[l]

    def build_spanning_table(self, root_id):
        stack = [root_id]
        covered_node_set = {root_id.split("_")[0]}
        results = []
        visited_set = set()
        while stack:
            i = len(stack) - 1
            while i >= 0:
                already_visited = False
                node_id = stack[i]
                can_go_further = False
                for child_id in self.links[node_id]:
                    if child_id.split("_")[0] in covered_node_set:
                        continue
                    tmp = self.get_id(stack + [child_id])
                    if tmp not in visited_set:
                        visited_set.add(tmp)
                        covered_node_set.add(child_id.split("_")[0])
                        stack.append(child_id)
                        i = len(stack) - 1
                        can_go_further = True
                        break
                    else:
                        already_visited = True
                if not can_go_further:
                    i = i - 1
            if not already_visited:
                potential = set(stack)
                is_new = True
                for res in results:
                    if potential.issubset(res):
                        is_new = False
                        break
                if is_new:
                    results.append(set(stack))
            covered_node_set.remove(stack[-1].split("_")[0])
            stack = stack[:-1]

        for r in results:
            print(r)

    # def build_spanning_table(self, root_id):
    #     def _dfs(node_id, results, cur_visted, visited_set, covered_node_set):
    #         can_travel = False
    #         for child_id in self.links[node_id]:
    #             if child_id.split("_")[0] in covered_node_set:
    #                 continue
    #             if sorted(cur_visted + [child_id]) not in visited_set:
    #                 can_travel = True
    #                 visited_set.add(sorted(cur_visted + [child_id]))
    #                 covered_node_set.add(child_id.split("_")[0])
    #                 _dfs(child_id, results, cur_visted + [child_id], visited_set, covered_node_set)
    #         if not can_travel and node_id == root_id:
    #             results.append(cur_visted)
    #             covered_node_set = {child_id.split("_")[0]}
    #             cur_visted = [root_id]


if __name__ == "__main__":
    etl = ETL("/Users/giangbui/Projects/pypfb/tests/pfb-data/test.avro", "participant")
    # etl.transform()
    # import pdb; pdb.set_trace()
    etl.links = {
        "A_1": ["B_1", "C_1", "C_2", "B_2"],
        "B_1": ["D_1"],
        "D_1": ["C_1"],
        "C_1": ["D_2", "B_3"],
        "B_2": [],
        "C_2": [],
        "D_2": [],
        "B_3": [],
    }
    # etl.build_spanning_table("participant_metalinguistics_monofilm")
    etl.build_spanning_table("A_1")
    # voc_tree_result = {}
    # etl.check_and_add_candidate(["a", "b", "c"], voc_tree_result)
    # etl.check_and_add_candidate(["a", "b", "d"], voc_tree_result)
    # etl.check_and_add_candidate(["a", "b"], voc_tree_result)
    # etl.check_and_add_candidate(["a", "b"], voc_tree_result)
    # print(voc_tree_result)
