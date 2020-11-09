import asyncio
from fastavro import reader
from aiohttp import ClientSession


class ETLHelper:
    """ Asynchronous file helper class"""

    def __init__(self, base_url, access_token):
        self.base_url = base_url
        self.headers = {"Content-Type": "application/json"}

    async def insert_document(self, index, document, id):
        """Asynchronous update authz field for did"""
        url = f"{self.base_url}/{index}/_doc/{id}"
        async with ClientSession() as session:
            async with session.put(url, json=document, headers=self.headers,) as r:
                r.raise_for_status()


class ETL:
    def __init__(self, base_url, access_token, pfbfile, root_name):
        self.base_url = base_url
        self.access_token = access_token
        self.pfbfile = pfbfile
        self.root_name = root_name

        self.schema_node_names = []
        self.links = {}
        self.root_node_ids = []
        self.spanning_tree_rows = []
        self.node_rows = {}
        self.schema = None

        self.helper = ETLHelper(base_url, access_token)

    def __iter__(self):
        pass

    def transform(self):
        with open(self.pfbfile, "rb") as fo:
            avro_reader = reader(fo)
            # read schema
            self.schema = avro_reader.next()
            self.schema_node_names = []
            for node in self.schema["object"]["nodes"]:
                self.schema_node_names.append(node["name"])
            self.schema_node_names = sorted(
                self.schema_node_names, key=lambda x: len(x), reverse=True
            )

            # iterate each record
            for record in avro_reader:
                self._process(record)

    def _process(self, record):
        data = record["object"]
        if record["name"] not in self.node_rows:
            self.node_rows[record["name"]] = []
        self.node_rows[record["name"]].append(data)
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
        def dfs(root_id, chosen_node_values):
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
                    if v not in visited and v in chosen_node_values:
                        visited.add(v)
                        stack.append(v)
            return visited

        def pick_k_th_node(k, node_name_list, node_values, chosen_node_values):
            """
            Pick the k-th candidate from node values
            """
            if k == len(node_name_list):
                solution = dfs(root_id, set(chosen_node_values))
                delete_list = []
                for r in self.spanning_tree_rows:
                    # ignore the spanning tree whose parent already exists
                    if solution.issubset(r):
                        return
                    # delete the spanning tree whose parent is about to be added
                    if r.issubset(solution):
                        delete_list.append(r)
                self.spanning_tree_rows.append(solution)
                for element in delete_list:
                    self.spanning_tree_rows.remove(element)
                return

            for node_value in node_values[node_name_list[k]]:
                pick_k_th_node(
                    k + 1,
                    node_name_list,
                    node_values,
                    chosen_node_values + [node_value],
                )

        node_name_list = set()
        node_values = {}
        for k, v in self.links.items():
            for e in [k] + v:
                node_name = e.split("_")[0]
                for name in self.schema_node_names:
                    if name in e:
                        node_name = name
                        break
                node_name_list.add(node_name)
                if node_name not in node_values:
                    node_values[node_name] = set()
                node_values[node_name].add(e)
        node_name_list = list(node_name_list)
        chosen_node_values = []
        pick_k_th_node(0, node_name_list, node_values, chosen_node_values)

    async def submit_data(self):
        for node_name, values in self.node_rows.items():
            for value in values:
                await self.helper.insert_document(
                    node_name, value, value["submitter_id"]
                )


if __name__ == "__main__":
    etl = ETL("http://localhost:9200", "", "tests/pfb-data/test.avro", "participant")
    etl.transform()
    etl.build_spanning_table("participant_metalinguistics_monofilm")
    asyncio.run(etl.submit_data())
