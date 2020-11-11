import asyncio
from fastavro import reader
from aiohttp import ClientSession


class ETLHelper:
    """ Asynchronous file helper class"""

    def __init__(self, base_url, access_token):
        self.base_url = base_url
        self.token = access_token
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}",
        }

    async def insert_document(self, index, document, id):
        """Asynchronous insert document to elasticsearch"""
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

        # self.schema_node_names = []
        self.links = {}
        self.root_node_ids = []
        self.spanning_tree_rows = []
        self.node_rows = {}
        # self.schema = None

        self.helper = ETLHelper(base_url, access_token)

    def __iter__(self):
        pass

    def transform(self):
        with open(self.pfbfile, "rb") as fo:
            avro_reader = reader(fo)
            # skip schema
            avro_reader.next()

            # for node in self.schema["object"]["nodes"]:
            #     self.schema_node_names.append(node["name"])
            # self.schema_node_names = sorted(
            #     self.schema_node_names, key=lambda x: len(x), reverse=True
            # )

            # iterate each record
            for record in avro_reader:
                self._process(record)

    def _process(self, record):
        data = record["object"]
        if "submitter_id" not in data:
            # skip the data that does not have submitter_id
            return
        if record["name"] not in self.node_rows:
            self.node_rows[record["name"]] = []
        self.node_rows[record["name"]].append(data)
        relations = record["relations"]
        submitter_id = data["submitter_id"]
        for relation in relations:
            if (
                self.root_name == relation["dst_name"]
                and relation["dst_id"] not in self.root_node_ids
            ):
                self.root_node_ids.append(relation["dst_id"])

            if (relation["dst_id"], relation["dst_name"]) not in self.links:
                self.links[(relation["dst_id"], relation["dst_name"])] = []
            self.links[(relation["dst_id"], relation["dst_name"])].append(
                (submitter_id, record["name"])
            )

    def find_all_node_ids(self, root):
        """
        Find all node ids that can be reached from root id

        Args:
            root_id(str): root submitter_id

        Returns:
            result(dict): group all submitter_id by the node name as the key
        """
        queue = [root]
        visited = {}
        idx = 0
        while idx < len(queue):
            submitter_id, node_name = queue[idx]

            unique_key = (submitter_id, node_name)
            visited[unique_key] = 1
            for node_id, node_name in self.links.get(unique_key, []):
                if (node_id, node_name) not in visited:
                    queue.append((node_id, node_name))
            idx += 1

        result = {}
        for submitted_id, node_name in visited.keys():
            if node_name not in result:
                result[node_name] = set()
            result[node_name].add(submitted_id)
        return result

    def build_spanning_table(self, root):
        def dfs(root, chosen_node_ids):
            """
            Spanning from the root with the current node ids or submitter id
            The result is the list of the nodes that can reach from the root
            """
            stack = [root]
            visited = set()
            visited.add(root)
            while stack:
                top = stack.pop()
                for v in self.links.get(top, []):
                    if v not in visited and v in chosen_node_ids:
                        visited.add(v)
                        stack.append(v)
            return visited

        def pick_k_th_node(k, node_name_list, node_ids, chosen_node_ids):
            """
            Pick the k-th candidate from node ids and find the largest connected component
            that contains the root node

            Args:
                k(int): the control value
                node_name_list(list): the list of node names that have paths to the root node
                node_ids(list): the list of submitter_id that have paths to the root_id
                chose_node_ids(list): keep track the current chosen node ids

            Return:
                None
            """
            if k == len(node_name_list):
                solution = dfs(root, set(chosen_node_ids))
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

            for node_value in node_ids[node_name_list[k]]:
                pick_k_th_node(
                    k + 1,
                    node_name_list,
                    node_ids,
                    chosen_node_ids + [(node_value, node_name_list[k])],
                )

        chosen_node_ids = []

        # Find all related node ids that have a path to the root_id
        related_node_ids = self.find_all_node_ids(root)
        node_name_list = [node_name for node_name in related_node_ids]
        pick_k_th_node(0, node_name_list, related_node_ids, chosen_node_ids)

    async def submit_data(self):
        for node_name, values in self.node_rows.items():
            for value in values:
                await self.helper.insert_document(
                    node_name, value, value["submitter_id"]
                )

        # TODO: implement batch insertion
        i = 0
        for row in self.spanning_tree_rows:
            submission_json = {}
            for (node_id, node_name) in row:
                submission_json[node_name] = node_id
            await self.helper.insert_document("spanning_tree_index", submission_json, i)
            i += 1


if __name__ == "__main__":
    etl = ETL(
        "http://localhost:9200", "", "./tests/pfb-data/gtexdictionary_data.avro", "case"
    )
    etl.transform()
    n = 0
    for root_id in etl.root_node_ids:
        print(f"{n}/{len(etl.root_node_ids)}")
        n = n + 1
        etl.build_spanning_table((root_id, etl.root_name))
    asyncio.run(etl.submit_data())
