import asyncio
from fastavro import reader
from aiohttp import ClientSession
from collections import OrderedDict
import yaml
from yaml import CLoader as Loader


MAPPING_INDEX_DEF = {
    "settings": {
        "index": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "analysis": {
                "tokenizer": {
                    "ngram_tokenizer": {
                        "type": "ngram",
                        "min_gram": 2,
                        "max_gram": 20,
                        "token_chars": ["letter", "digit"],
                    }
                },
                "analyzer": {
                    "ngram_analyzer": {
                        "type": "custom",
                        "tokenizer": "ngram_tokenizer",
                        "filter": ["lowercase"],
                    },
                    "search_analyzer": {
                        "type": "custom",
                        "tokenizer": "keyword",
                        "filter": "lowercase",
                    },
                },
            },
        }
    }
}


class ETLHelper:
    """ Asynchronous file helper class"""

    session = None

    def __init__(self, base_url, access_token):
        self.base_url = base_url
        self.token = access_token
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}",
        }

    @classmethod
    def get_session(cls):
        if cls.session is None:
            cls.session = ClientSession()
        return cls.session

    @classmethod
    def close_session(cls):
        if cls.session is not None:
            return cls.session.close()

    async def check_index_exist(self, index):
        session = ETLHelper.get_session()
        async with session.get(f"{self.base_url}/{index}", headers=self.headers,) as r:
            r.raise_for_status()
            if r.status == 200:
                return True
            return False

    async def insert_document(self, url, document):
        """Asynchronous insert document to elasticsearch"""
        # url = f"{self.base_url}/{index}/{type}/{id}"
        session = ETLHelper.get_session()
        async with session.put(url, json=document, headers=self.headers,) as r:
            r.raise_for_status()


class ETL:
    def __init__(self, base_url, access_token, pfbfile, root_name, etl_mapping):
        self.base_url = base_url
        self.access_token = access_token
        self.pfbfile = pfbfile
        self.root_name = root_name
        self.etl_mapping = etl_mapping

        self.links = {}
        self.root_node_ids = []
        self.spanning_tree_rows = []
        self.node_rows = OrderedDict()

        self.helper = ETLHelper(base_url, access_token)

    async def etl(self):
        try:
            await self.preliminary_check()
            self.extract()
            self.transform()
            await self.load_to_es()
        finally:
            await ETLHelper.close_session()

    async def preliminary_check(self):
        index_exist = await self.helper.check_index_exist(self.root_name)
        if index_exist:
            print(
                f"The index of {self.root_name} already exists. Please delete it to continue"
            )
            exit(1)

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
            self.node_rows[record["name"]] = OrderedDict()
        self.node_rows[record["name"]][data["submitter_id"]] = data
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
        n = 1
        for root_id in self.root_node_ids:
            if n % 10 == 0 or n == len(self.root_node_ids):
                print(
                    f"Progress: solve {n}/{len(self.root_node_ids)} spanning tree sub-problems"
                )
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
        visited = OrderedDict()
        idx = 0
        while idx < len(queue):
            submitter_id, node_name = queue[idx]

            unique_key = (submitter_id, node_name)
            visited[unique_key] = 1
            for node_id, node_name in self.links.get(unique_key, []):
                if (node_id, node_name) not in visited:
                    queue.append((node_id, node_name))
            idx += 1

        result = OrderedDict()
        for submitted_id, node_name in visited.keys():
            if node_name not in result:
                result[node_name] = set()
            result[node_name].add(submitted_id)
        return result

    async def load_to_es(self):
        """
        (Pdb) etl_mapping["mappings"][0]["props"] = [{'name': 'submitter_id'}, {'name': 'site'}, {'name': 'cohort_id'}]
        (Pdb) etl_mapping["mappings"][0]["flatten_props"] = [{'node': 'demographic', 'props': [{'name': 'gender'}, {'name': 'race'}, {'name': 'ethnicity'}, {'name': 'weight'}, {'name': 'hispanic_subgroup'}]}]
        (Pdb) etl_mapping["mappings"][0]["aggregated_props"]
        [{'name': 'cigar_amount', 'node': 'exposure', 'fn': 'avg'}, {'name': 'age_at_hdl1', 'node': 'lab_result', 'fn': 'max'}, {'name': '_lab_result_count', 'path': 'lab_result', 'fn': 'count'}]
        (Pdb)
        """
        with open(self.etl_mapping, "r") as fopen:
            etl_mapping = yaml.load(fopen, Loader=Loader)

        assert (
            self.root_name == etl_mapping["mappings"][0]["root"]
        ), f"The root of the ETL config file does not match"
        related_nodes = {self.root_name}

        for element in etl_mapping["mappings"][0].get("flatten_props", []):
            related_nodes.add(element["node"])

        for element in etl_mapping["mappings"][0].get("aggregated_props", []):
            related_nodes.add(element["node"])

        root_mapping = {}

        for row in self.spanning_tree_rows:
            row_related_nodes = set([node_name for _, node_name in row])
            if not related_nodes.issubset(row_related_nodes):
                continue
            for element in row:
                if element[1] != self.root_name:
                    continue
                if element[0] in root_mapping:
                    root_mapping[element[0]].append(row)
                else:
                    root_mapping[element[0]] = [row]

        is_first_submission = True
        for root_submitter_id, related_nodes in root_mapping.items():
            submission_json = {}
            aggregation_data = []
            for related_node in related_nodes:
                for node_props in etl_mapping["mappings"][0].get("props", []):
                    submission_json.update(
                        self._get_root_props(node_props, related_node)
                    )
                for node_props in etl_mapping["mappings"][0].get("flatten_props", []):
                    submission_json.update(
                        self._get_flatten_props(node_props, related_node)
                    )
                for node_props in etl_mapping["mappings"][0].get(
                    "aggregated_props", []
                ):
                    aggregation_data.append(
                        self._get_aggregation_props(node_props, related_node)
                    )

            for element in aggregation_data:
                for field, info in element.items():
                    # element = {field: (value, fn)}
                    if info[0] is None:
                        continue
                    if info[1] == "max":
                        submission_json[field] = (
                            max(submission_json[field], info[0])
                            if field in submission_json
                            else info[0]
                        )
                    if info[1] == "min":
                        submission_json[field] = (
                            min(submission_json[field], info[0])
                            if field in submission_json
                            else info[0]
                        )
                    if info[1] == "avg":
                        submission_json[field] = (
                            (
                                submission_json[field][0] + info[0],
                                submission_json[field][1] + 1,
                            )
                            if field in submission_json
                            else (info[0], 1)
                        )
                    if info[1] == "count":
                        pass

            for k, v in submission_json.items():
                if isinstance(v, tuple):
                    submission_json[k] = v[0] * 1.0 / v[1]

            if is_first_submission:
                indexd_template = self.create_index_template(submission_json)
                await self.helper.insert_document(
                    f"{self.base_url}/{self.root_name}", indexd_template
                )

            await self.helper.insert_document(
                f"{self.base_url}/{self.root_name}/{self.root_name}/{root_submitter_id}",
                submission_json,
            )
            is_first_submission = False

    def _get_root_props(self, node_props, related_node):
        """
        Get ETL config root props and compute their values

        Args:
            node_props(dict): root props from etl mapping config
            {
                'name': 'submitter_id'
            }

            related_node(dict): the node_ids contributing to the value of the root props
            {
                ("medication_locustelle_uninterlarded", "medication"),
                ("demographic_strickless_johannite", "demographic"),
            }

        Returns:
            res(dict): {root_prop: value}

        """
        res = {}
        for element in related_node:
            if element[1] == self.root_name:
                res[node_props["name"]] = self.node_rows[self.root_name][element[0]][
                    node_props["name"]
                ]
        return res

    def _get_flatten_props(self, node_props, related_node):
        """
        Get ETL config flatten props and compute their values

        Args:
            node_props(dict): flatten props from etl mapping config
            {
                "node": "demographic",
                "props": [
                    {"name": "gender"},
                    {"name": "race"},
                    {"name": "ethnicity"},
                    {"name": "weight"},
                    {"name": "hispanic_subgroup"},
                ],
            }

            related_node(dict): the node_ids contributing to the value of flatten props
            {
                ("medication_locustelle_uninterlarded", "medication"),
                ("demographic_strickless_johannite", "demographic"),
            }

        Returns:
            res(dict): {flatten_prop: value}

        """

        res = {}
        node_name = node_props["node"]
        props = node_props["props"]
        for element in related_node:
            if element[1] == node_props["node"]:
                data = self.node_rows[node_name][element[0]]
                for prop in props:
                    res[prop["name"]] = data[prop["name"]]
        return res

    def _get_aggregation_props(self, node_props, related_node):
        """
        Get ETL config aggregation props and compute their values

        Args:
            node_props(dict): flatten props from etl mapping config
                {
                    "name": "cigar_amount",
                    "node": "exposure",
                    "fn": "avg"
                }

            related_node(dict): the node_ids contributing to the value of aggregation props
            {
                ("medication_locustelle_uninterlarded", "medication"),
                ("demographic_strickless_johannite", "demographic"),
            }

        Returns:
            res(dict): {aggregation_prop: (value, [avg|max|min|count])}
        """
        res = {}
        node_name = node_props["node"]
        field = node_props["name"]
        fn = node_props["fn"]
        if fn == "count":
            return res
        for element in related_node:
            if element[1] == node_props["node"]:
                data = self.node_rows[node_name][element[0]]
                res[field] = (data[field], fn)
        return res

    def create_index_template(self, submission_json):
        """ Create index template"""
        result = {}
        for key, value in submission_json.items():
            _type = None
            if isinstance(value, str):
                _type = "keyword"
            elif isinstance(value, int):
                _type = "integer"
            elif isinstance(value, float):
                _type = "float"
            if not _type:
                _type = "keyword"
            result[key] = {
                "type": _type,
                "fields": {
                    "analyzed": {
                        "type": "text",
                        "analyzer": "ngram_analyzer",
                        "search_analyzer": "search_analyzer",
                        "term_vector": "with_positions_offsets",
                    }
                },
            }

        mapping_json = MAPPING_INDEX_DEF
        mapping_json["mappings"] = {self.root_name: {"properties": result}}

        return mapping_json
