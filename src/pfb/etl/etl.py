from fastavro import reader


class ETL:
    def __init__(self, pfbfile, root_name):
        self.pfbfile = pfbfile
        self.root_name = root_name
        self.links = {}
        self.root_node_ids = []
        self.spanning_tree_rows = []

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
            if relation["dst_id"] not in self.links:
                self.links[relation["dst_id"]] = []
            if (
                relation["dst_id"].split("_")[0] == self.root_name
                and relation["dst_id"] not in self.root_node_ids
            ):
                self.root_node_ids.append(relation["dst_id"])
            self.links[relation["dst_id"]].append(submitter_id)

    def build_spanning_table(self, root_id):
        def dfs(root_id, cur_values):
            """
            Spanning from the root with the current node values
            The result is the list of the nodes that can reach from the root
            """
            stack = [root_id]
            visited = set()
            visited.add(root_id)
            while stack:
                top = stack.pop()
                for v in self.links.get(top, []):
                    if v not in visited and v in cur_values:
                        visited.add(v)
                        stack.append(v)
            return visited

        def pick_k_candidate(k, node_name_list, node_values, cur_values):
            """
            Pick the k-th candidate from node values
            """
            if k == len(node_name_list):
                solution = dfs(root_id, set(cur_values))
                delete_list = []
                for r in self.spanning_tree_rows:
                    if solution.issubset(r):
                        return
                    if r.issubset(solution):
                        delete_list.append(r)
                self.spanning_tree_rows.append(solution)
                for element in delete_list:
                    self.spanning_tree_rows.remove(element)
                return

            for node_value in node_values[node_name_list[k]]:
                pick_k_candidate(
                    k + 1, node_name_list, node_values, cur_values + [node_value]
                )

        node_name_list = set()
        node_values = {}
        for k, v in self.links.items():
            for e in [k] + v:
                node_name = e.split("_")[0]
                node_name_list.add(node_name)
                if node_name not in node_values:
                    node_values[node_name] = set()
                node_values[node_name].add(e)
        node_name_list = list(node_name_list)
        cur_values = []
        pick_k_candidate(0, node_name_list, node_values, cur_values)


if __name__ == "__main__":
    etl = ETL("tests/pfb-data/test.avro", "participant")
    etl.transform()
    etl.build_spanning_table("participant_metalinguistics_monofilm")
    print("=====================================================")
    for r in etl.spanning_tree_rows:
        print(r)
