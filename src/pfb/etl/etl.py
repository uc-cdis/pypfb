import asyncio
from fastavro import reader
from aiohttp import ClientSession


class ETLHelper:
    """Asynchronous file helper class"""

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
            async with session.put(
                url,
                json=document,
                headers=self.headers,
            ) as r:
                r.raise_for_status()


class ETL:
    def __init__(self, base_url, access_token, pfbfile, root_name):
        self.base_url = base_url
        self.access_token = access_token
        self.pfbfile = pfbfile
        self.root_name = root_name

        self.links = {}
        self.root_node_ids = []
        self.spanning_tree_rows = []
        self.node_rows = {}

        self.helper = ETLHelper(base_url, access_token)

    async def etl(self):
        self.extract()
        self.transform()
        await self.load_to_es()

    def extract(self):
        with open(self.pfbfile, "rb") as fo:
            avro_reader = reader(fo)
            # skip schema
            avro_reader.next()
            # iterate each record
            for record in avro_reader:
                self._process(record)

    def _process(self, record):
        """extract data from record"""
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

    def transform(self):
        """transform"""
        n = 0
        for root_id in self.root_node_ids:
            print(f"{n}/{len(self.root_node_ids)}")
            n = n + 1
            self._build_spanning_table((root_id, self.root_name))

    def _build_spanning_table(self, root):
        """
        Building a spanning table starting from the root node. A row of
        the table demonstrates one relationship of all nodes to the root
        """

        def dfs(root, selected_nodes):
            """
            Finding the connected component that containt the root

            Depth first search: Starting from the root on the tree created by the selected root,
            traveling to visit all the reachable nodes

            Args:
                root(tube): tuple of (submitter_id, node_name)
                selected_nodes(list(tube)): list of tubes of (submitter_id, node_name)

            Returns:
                visited(tube): list of (submitter_id, node_name) can be reached from the root
            """

            stack = [root]
            visited = set()
            visited.add(root)
            while stack:
                top = stack.pop()
                for v in self.links.get(top, []):
                    if v not in visited and v in selected_nodes:
                        visited.add(v)
                        stack.append(v)
            return visited

        def pick_k_th_node(k, node_name_list, node_ids, selected_nodes):
            """
            Pick the k-th candidate from node ids and find the largest connected component
            that contains the root node as a potential solution

            Args:
                k(int): the control variable
                node_name_list(list): A list of node names that have paths to the root node
                node_ids(dict): A dictionary with node names as keys and sets of their corresponding submitter_id as values
                selected_nodes(list): list of tube(submitter_id, node_name) for keeping track the current chosen node ids

            Return:
                None
            """
            if k == len(node_name_list):
                solution = dfs(root, set(selected_nodes))
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
                    selected_nodes + [(node_value, node_name_list[k])],
                )

        selected_nodes = []

        # Find all related node ids that have a path to the root_id
        related_node_ids = self.find_all_node_ids(root)
        node_name_list = [node_name for node_name in related_node_ids]
        pick_k_th_node(0, node_name_list, related_node_ids, selected_nodes)

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

    async def load_to_es(self):
        """submit data to es database"""

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
